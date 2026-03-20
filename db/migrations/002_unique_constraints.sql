-- Add unique constraints required for idempotent upserts

ALTER TABLE agencies
    ADD CONSTRAINT agencies_external_portal_id_key UNIQUE (external_portal_id);

ALTER TABLE jobs
    ADD CONSTRAINT jobs_source_url_key UNIQUE (source_url);

ALTER TABLE job_details
    ADD CONSTRAINT job_details_job_id_key UNIQUE (job_id);

ALTER TABLE job_line_items
    ADD CONSTRAINT job_line_items_job_id_item_number_key UNIQUE (job_id, item_number);

ALTER TABLE job_media
    ADD CONSTRAINT job_media_job_id_file_url_key UNIQUE (job_id, file_url);

ALTER TABLE companies
    ADD CONSTRAINT companies_name_location_state_key UNIQUE (name, location_state);

ALTER TABLE awards
    ADD CONSTRAINT awards_job_id_key UNIQUE (job_id);