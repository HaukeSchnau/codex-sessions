# Secrets

`dev.enc.yaml` is encrypted with SOPS and decrypted by `scripts/dev-env` into a local ignored `.env` file.

The default local age identity lives at `.sops/age.key`, which is intentionally ignored. Set `SOPS_AGE_KEY_FILE` when decrypting with a different identity.
