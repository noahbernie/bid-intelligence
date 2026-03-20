ALTER TABLE bids
    ADD CONSTRAINT bids_job_id_company_id_key UNIQUE (job_id, company_id);