# instantGMP

ETL pipeline for the instantGMP inventory management API → Snowflake.

Extracted from [sauron](https://github.com/Bear-Cognition/sauron) for standalone deployment.

## Layout

```
instantgmp/          # API clients, Pydantic models, run logic
src/                 # Snowflake, AWS, secrets, HTTP utilities
main.py              # CLI entrypoint
provisioning.py      # One-off schema/credentials provisioning
requirements.txt     # Python dependencies
```

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export INSTANT_GMP_CONFIG='{"f_number":"F001226","auth":{"api_user":"...","api_pass":"...","base_url":"..."}}'
python main.py
```

Or call `run` directly:

```bash
python -c "
import json, os
from instantgmp.config import FlowParams
from instantgmp.deploy import run
params = FlowParams(**json.loads(os.environ['INSTANT_GMP_CONFIG']))
run(params)
"
```

Snowflake credentials are loaded from Prefect secret blocks (`prod-snowflake-role` or `hipa-snowflake-role`). Each block should be JSON with `account`, `user`, `password`, and `warehouse`.
