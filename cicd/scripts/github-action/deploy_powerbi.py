import os
import argparse

from azure.identity import ClientSecretCredential

from fabric_cicd import FabricWorkspace, publish_all_items, change_log_level

change_log_level("DEBUG")

parser = argparse.ArgumentParser()

parser.add_argument("--aztenantid", required=True)
parser.add_argument("--azclientid", required=True)
parser.add_argument("--azspsecret", required=True)
parser.add_argument("--workspaceid", required=True)
parser.add_argument("--target_env", required=True)


args = parser.parse_args()


print("Authenticating Fabric")


credential = ClientSecretCredential(
    tenant_id=args.aztenantid, client_id=args.azclientid, client_secret=args.azspsecret
)


repository_directory = os.path.join(
    os.getenv("GITHUB_WORKSPACE", os.getcwd()), "fabric"
)


print(f"Repository: {repository_directory}")


# ONLY POWER BI
item_types = ["SemanticModel", "Report"]


workspace = FabricWorkspace(
    workspace_id=args.workspaceid,
    environment=args.target_env,
    repository_directory=repository_directory,
    item_type_in_scope=item_types,
    token_credential=credential,
)


print("Publishing PowerBI artifacts...")


publish_all_items(workspace)


print("PowerBI deployment completed")
