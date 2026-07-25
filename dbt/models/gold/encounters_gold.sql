select
  encounter_id,
  patient_id_token,
  encounter_time,
  diagnosis_code,
  record_updated_at
from {{ source('silver', 'encounters_staging') }}
