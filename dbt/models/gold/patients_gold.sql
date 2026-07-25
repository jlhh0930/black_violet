select
  patient_id_token,
  email_masked,
  phone_hash,
  birth_date_masked,
  record_updated_at
from {{ source('silver', 'patients_staging') }}
