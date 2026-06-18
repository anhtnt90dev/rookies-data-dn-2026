import os,argparse, requests, ast
from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items,change_log_level,append_feature_flag

def get_workspace_id(p_ws_name, p_token):
    url = "https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {p_token.token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    ws_id =''
    if response.status_code == 200:
        workspaces = response.json()["value"]
        for workspace in workspaces:
            if workspace["displayName"] == p_ws_name:
                ws_id = workspace["id"] 
                return workspace["id"]
        if ws_id == '':
            return f"Error: Workspace {p_ws_name} could not found."
    else:
        return f"Error: {response.status_code}, {response.text}"

# --- Feature Flags and Logging ---
append_feature_flag("enable_shortcut_publish")
# set log level
change_log_level("DEBUG")

# parse arguments from yaml pipeline. These are typically secrets from a variable group linked to an Azure Key Vault
parser = argparse.ArgumentParser(description='Process Azure Pipeline arguments.')
parser.add_argument('--aztenantid',type=str, help= 'tenant ID')
parser.add_argument('--azclientid',type=str, help= 'SP client ID')
parser.add_argument('--azspsecret',type=str, help= 'SP secret')
parser.add_argument('--target_env',type=str, help= 'target environment')
parser.add_argument('--workspaceid',type=str, help= 'Optional: workspace GUID to deploy to')
parser.add_argument('--workspacename',type=str, help= 'Optional: workspace display name to resolve to GUID')

args = parser.parse_args()

print('Obtaining token...')
token_credential = ClientSecretCredential(client_id=args.azclientid, client_secret=args.azspsecret, tenant_id=args.aztenantid)

tgtenv = args.target_env
print(f'Target environment set to {tgtenv}')

# determine the target workspace using the variable group which stores the target workspace name in a variable with the naming convention "[tgtenv]WorkspaceName"
ws_name = f'{tgtenv}WorkspaceName'
ws_env_var = ws_name.upper()
print(f'Variable group to determine workspace is set to {ws_name} (env: {ws_env_var})')

resource = 'https://api.fabric.microsoft.com/'
scope = f'{resource}.default'
print(f'scope set to {scope}')
token = token_credential.get_token(scope)
if args.workspaceid:
    wks_id = args.workspaceid
    print(f"Using workspace id provided via argument: {wks_id}")
elif args.workspacename:
    print(f"Resolving workspace name provided via argument: {args.workspacename}")
    lookup_response = get_workspace_id(args.workspacename, token)
    if lookup_response.startswith("Error"):
        errmsg = f"{lookup_response}. Workspace name passed via --workspacename may be incorrect."
        raise ValueError(errmsg)
    wks_id = lookup_response
    print(f"Workspace ID for {args.workspacename} set to {wks_id}")
else:
    # try environment variable fallback
    workspace_name = os.environ.get(ws_env_var)
    if not workspace_name:
        raise KeyError(f"Required environment variable '{ws_env_var}' not found. Set it or pass --workspaceid/--workspacename.")
    print(f'Obtaining GUID for {workspace_name}')
    lookup_response = get_workspace_id(workspace_name, token)
    if lookup_response.startswith("Error"):
        errmsg=f"{lookup_response}. Perhaps workspace name is set incorrectly in the variable group or does not map to environment name + 'WorkspaceName'"
        raise ValueError(errmsg)
    else:
        wks_id = lookup_response
        print(f"Workspace ID for {workspace_name} set to {wks_id}")


# set repo folder based on the variable group value of gitDirectory
repository_directory = os.path.join(
    os.getenv("GITHUB_WORKSPACE", os.getcwd()),
    "fabric"
)

print(f"Repository directory: {repository_directory}")

print(repository_directory)

# convert the item types argument into a valid list
list_item_types = ["Notebook","DataPipeline","Lakehouse","SemanticModel","Report","VariableLibrary"]

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=wks_id,
    environment=tgtenv,
    repository_directory=repository_directory,
    item_type_in_scope=list_item_types,
    token_credential=token_credential,
)

print(f'Publish branch to workspace...')
publish_all_items(target_workspace)

# Unpublish orphaned items from the workspace
unpublish_all_orphan_items(target_workspace)