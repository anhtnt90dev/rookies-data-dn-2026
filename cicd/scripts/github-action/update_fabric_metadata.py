import json
import re
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ENV = os.getenv("DEPLOY_ENV")


CONFIG = {
    "dev": {
        "workspace_id": os.getenv("FABRIC_WORKSPACE_DEV_ID"),
        "lakehouse_id": os.getenv("FABRIC_LAKEHOUSE_DEV_ID"),
        "lakehouse_name": os.getenv("FABRIC_LAKEHOUSE_DEV_NAME"),
    },
    "prod": {
        "workspace_id": os.getenv("FABRIC_WORKSPACE_PROD_ID"),
        "lakehouse_id": os.getenv("FABRIC_LAKEHOUSE_PROD_ID"),
        "lakehouse_name": os.getenv("FABRIC_LAKEHOUSE_PROD_NAME"),
    }
}


def extract_meta(content):

    pattern = r"# META \{.*?# META \}"

    match = re.search(
        pattern,
        content,
        re.DOTALL
    )

    if not match:
        return None, None


    meta_block = match.group()


    json_text = "\n".join(
        line.replace("# META", "").strip()
        for line in meta_block.splitlines()
    )


    return (
        meta_block,
        json.loads(json_text)
    )


def build_meta_block(meta):

    json_text = json.dumps(
        meta,
        indent=2
    )

    return "\n".join(
        f"# META {line}"
        for line in json_text.splitlines()
    )


def update_notebook(file_path):

    content = Path(file_path).read_text(
        encoding="utf-8"
    )


    old_meta, meta = extract_meta(content)


    if meta is None:
        print(
            f"Skip {file_path}: no META"
        )
        return


    lakehouse = (
        meta
        .get("dependencies", {})
        .get("lakehouse")
    )


    if not lakehouse:
        print(
            f"Skip {file_path}: no lakehouse dependency"
        )
        return


    env_config = CONFIG[ENV]


    lakehouse["default_lakehouse"] = (
        env_config["lakehouse_id"]
    )


    lakehouse["default_lakehouse_name"] = (
        env_config["lakehouse_name"]
    )


    lakehouse["default_lakehouse_workspace_id"] = (
        env_config["workspace_id"]
    )


    known_lakehouses = (
        lakehouse
        .get("known_lakehouses", [])
    )


    for item in known_lakehouses:
        item["id"] = (
            env_config["lakehouse_id"]
        )


    new_meta = build_meta_block(meta)


    updated_content = content.replace(
        old_meta,
        new_meta
    )


    Path(file_path).write_text(
        updated_content,
        encoding="utf-8"
    )


    print(
        f"Updated: {file_path}"
    )


def update_all_notebooks():

    notebooks = list(
        Path("fabric")
        .rglob("notebook-content.py")
    )


    print(
        f"Found {len(notebooks)} notebooks"
    )


    for notebook in notebooks:

        update_notebook(notebook)


if __name__ == "__main__":

    update_all_notebooks()