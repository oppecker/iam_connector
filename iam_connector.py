import boto3
import botocore
from boto3.dynamodb.conditions import Key
from expiire.aws.cloud_user_manager import CloudUserManager

def lambda_handler(event, context):
    profiles = get_all_account_profiles(db_name='dynamodb', table_name='expiire')
    for profile in [profile for profile in profiles]:
        try:
            connector = IAM_CONNECTOR(
                db_name='dynamodb',
                table_name='expiire',
                company_id=profile['company_id'],
                account_id=profile['account_id'],
                account_number=profile['account_number'],
                role_arn=profile['iam_arn'],
            )
            user_names = [name for name in connector.main()]
        
        except botocore.exceptions.ClientError as error:
            print(f"Error with client for profile: {profile}")
        
        except botocore.exceptions.ParamValidationError as error:
            print(f"Error with parameters for profile: {profile}")
            
    
def get_all_account_profiles(db_name, table_name):
    dynamodb = boto3.resource(db_name)
    table = dynamodb.Table(table_name)
    scan_kwargs = {
        'FilterExpression': Key('PK').begins_with('Company#') & Key('SK').begins_with('#AccountProfile#')
    }
    response = table.scan(**scan_kwargs)
    profiles = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **scan_kwargs)
        profiles.extend(response['Items'])
        
    for profile in profiles:
        yield {
            'iam_arn': profile['iam_arn'],
            'company_id': profile['company_id'],
            'account_id': profile['account_id'],
            'account_number': profile['account_number'],
        }
    
class IAM_CONNECTOR():
    def __init__(self, db_name, table_name, company_id, account_id, account_number, role_arn):
        dynamodb = boto3.resource(db_name)
        self.table = dynamodb.Table(table_name)
        
        self.company_id = company_id
        self.account_id = account_id
        self.account_number = account_number
        
        self.client = self.create_boto3_client(role_arn)
        self.cloud_user_manager = CloudUserManager()
        
    def create_boto3_client(self, role_arn):
        sts_connection = boto3.client('sts')
        acct_b = sts_connection.assume_role(
            RoleArn=role_arn,
            RoleSessionName="cross_acct_lambda"
        )
    
        return boto3.client(
            'iam',
            aws_access_key_id=acct_b['Credentials']['AccessKeyId'],
            aws_secret_access_key=acct_b['Credentials']['SecretAccessKey'],
            aws_session_token=acct_b['Credentials']['SessionToken'],
        )

    def main(self):
        # Step 1: Get List of Users
        response = self.client.list_users()

        # Step 2: Add Users to Table If they Don't Exist
        for user in [user['UserName'] for user in response['Users']]:
            if not self.cloud_user_manager.check_if_user_exists(
                name=user,
                company_id=self.company_id,
                account_id=self.account_id,
            ):
                print(f"creating user: {user}")
                self.cloud_user_manager.new_user(
                    company_id = self.company_id,
                    account_id = self.account_id,
                    name = user,
                    account_number = self.account_number,
                )
            else:
                print(f"user already exists: {user}")
            yield user
