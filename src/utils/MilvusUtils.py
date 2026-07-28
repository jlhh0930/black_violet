from __future__ import annotations

from dataclasses import dataclass
from glob import glob
from typing import Any

import json
import ollama
from ollama import ChatResponse, chat
from pymilvus import MilvusClient
from tqdm import tqdm


@dataclass(frozen=True)
class RagConfig:
    docs_glob: str = "milvus_docs/en/faq/*.md"
    milvus_uri: str = "./milvus_demo.db"
    collection_name: str = "my_rag_collection"

    chat_model: str = "llama3.2"
    embedding_model: str = "qwen3-embedding"

    metric_type: str = "IP"  # Inner product distance
    consistency_level: str = "Bounded"  # "Strong" | "Session" | "Bounded" | "Eventually"


class MilvusRagPipeline:
    """
    RAG pipeline:
      - load markdown fragments from docs_glob
      - embed with config.embedding_model
      - create/ensure Milvus collection
      - insert vectors + text
      - retrieve top-k via similarity search
      - answer using Ollama chat model
    """

    def __init__(self, config: RagConfig):
        self.config = config

        self.milvus_client = MilvusClient(uri=self.config.milvus_uri)
        self.embedding_dim: int | None = None

    # ----------------------------
    # Data prep / ingestion
    # ----------------------------
    def load_text_lines(self) -> list[str]:
        text_lines: list[str] = []
        for file_path in glob(self.config.docs_glob, recursive=True):
            with open(file_path, "r") as f:
                file_text = f.read()
            text_lines += file_text.split("# ")
        return text_lines

    def split_documents(
        self, texts: list[str], *, min_chars: int = 1, strip: bool = True
    ) -> list[str]:
        """
        Optional cleanup step for data engineers.
        """
        out: list[str] = []
        for t in texts:
            if strip:
                t = t.strip()
            if len(t) >= min_chars:
                out.append(t)
        return out

    def embed_text(self, text: str) -> list[float]:
        response = ollama.embeddings(model=self.config.embedding_model, prompt=text)
        return response["embedding"]

    def get_embedding_dim(self, sample_text: str = "This is a test") -> int:
        test_embedding = self.embed_text(sample_text)
        return len(test_embedding)

    def ensure_collection(self, embedding_dim: int, *, drop_if_exists: bool = False) -> None:
        if drop_if_exists and self.milvus_client.has_collection(self.config.collection_name):
            self.milvus_client.drop_collection(self.config.collection_name)

        if not self.milvus_client.has_collection(self.config.collection_name):
            self.milvus_client.create_collection(
                collection_name=self.config.collection_name,
                dimension=embedding_dim,
                metric_type=self.config.metric_type,
                consistency_level=self.config.consistency_level,
            )

    def make_insert_records(
        self,
        texts: list[str],
        *,
        start_id: int = 0,
        id_field: str = "id",
        vector_field: str = "vector",
        text_field: str = "text",
    ) -> list[dict[str, Any]]:
        """
        Build the list of dicts expected by MilvusClient.insert().
        """
        records: list[dict[str, Any]] = []
        for i, line in enumerate(texts, start=start_id):
            records.append(
                {
                    id_field: i,
                    vector_field: self.embed_text(line),
                    text_field: line,
                }
            )
        return records

    def ingest(
        self,
        *,
        drop_if_exists: bool = True,
        min_chars: int = 1,
        sample_text: str = "This is a test",
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """
        End-to-end ingestion:
          - load docs
          - optional filtering
          - ensure collection
          - embed + insert
        Returns summary stats useful for pipeline logs.
        """
        raw = self.load_text_lines()
        texts = self.split_documents(raw, min_chars=min_chars)

        self.embedding_dim = self.get_embedding_dim(sample_text=sample_text)
        self.ensure_collection(self.embedding_dim, drop_if_exists=drop_if_exists)

        data: list[dict[str, Any]] = []
        iterator = texts
        if show_progress:
            iterator = tqdm(texts, desc="Creating embeddings")

        for i, line in enumerate(iterator):
            data.append({"id": i, "vector": self.embed_text(line), "text": line})

        self.milvus_client.insert(collection_name=self.config.collection_name, data=data)

        return {
            "collection_name": self.config.collection_name,
            "ingested_chunks": len(texts),
            "embedding_dim": self.embedding_dim,
            "metric_type": self.config.metric_type,
            "consistency_level": self.config.consistency_level,
        }

    # ----------------------------
    # Index / lifecycle helpers
    # ----------------------------
    def collection_exists(self) -> bool:
        return self.milvus_client.has_collection(self.config.collection_name)

    def drop_collection(self) -> None:
        if self.collection_exists():
            self.milvus_client.drop_collection(self.config.collection_name)

    # ----------------------------
    # Retrieval
    # ----------------------------
    def retrieve(
        self,
        question: str,
        *,
        limit: int = 3,
        output_fields: list[str] | None = None,
        search_params: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        if output_fields is None:
            output_fields = ["text"]
        if search_params is None:
            # MilvusClient.search passes through to Milvus; keep metric_type in sync.
            search_params = {"metric_type": self.config.metric_type, "params": {}}

        search_res = self.milvus_client.search(
            collection_name=self.config.collection_name,
            data=[self.embed_text(question)],
            limit=limit,
            search_params=search_params,
            output_fields=output_fields,
        )

        return [(res["entity"]["text"], res["distance"]) for res in search_res[0]]

    def build_context(self, retrieved: list[tuple[str, float]], *, joiner: str = "\n") -> str:
        return joiner.join([line for line, _ in retrieved])

    # ----------------------------
    # Q&A
    # ----------------------------
    def build_prompts(self, question: str, context: str) -> tuple[str, str]:
        system_prompt = (
            "Human: You are an AI assistant. You are able to find answers to the "
            "questions from the contextual passage snippets provided."
        )
        user_prompt = f"""
            Use the following pieces of information enclosed in <context> tags to provide an answer
            to the question enclosed in <question> tags.
            <context>
            {context}
            </context>
            <question>
            {question}
            </question>
            """.strip()
        return system_prompt, user_prompt

    def answer(
        self,
        question: str,
        *,
        limit: int = 3,
        debug: bool = False,
    ) -> dict[str, Any]:
        retrieved = self.retrieve(question, limit=limit)
        context = self.build_context(retrieved)

        system_prompt, user_prompt = self.build_prompts(question=question, context=context)

        response: ChatResponse = chat(
            model=self.config.chat_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        result = {
            "question": question,
            "retrieved": [{"text": t, "distance": d} for (t, d) in retrieved],
            "context": context,
            "answer": response["message"]["content"],
        }

        if debug:
            print("Retrieved (text, distance):")
            print(json.dumps(result["retrieved"], indent=2))

        return result


if __name__ == "__main__":
    config = RagConfig(
        docs_glob="milvus_docs/en/faq/*.md",
        milvus_uri="./milvus_demo.db",
        collection_name="my_rag_collection",
        chat_model="llama3.2",
        embedding_model="qwen3-embedding",
        metric_type="IP",
        consistency_level="Bounded",
    )

    rag = MilvusRagPipeline(config)

    # Rebuild from scratch
    stats = rag.ingest(drop_if_exists=True, min_chars=1)
    print("Ingest stats:", stats)

    q = "How is data stored in milvus?"
    out = rag.answer(q, limit=3, debug=True)
    print(out["answer"])
