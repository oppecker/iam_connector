import boto3
import uuid
import json
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    connector = IAM_CONNECTOR(
        db_name='dynamodb',
        table_name='CIAM_DEMO',
        company_id=event['company'],
        account_id=event['account'],
        account_number=event['account_number'],
    )
    user_names = connector.main()

    return {
        'statusCode': 200,
        'body': json.dumps(user_names)
    }

class IAM_CONNECTOR():
    def __init__(self, db_name, table_name, company_id, account_id, account_number):
        # Step 1: Get Credentials From DB
        dynamodb = boto3.resource(db_name)
        self.table = dynamodb.Table(table_name)

        self.company_id = company_id
        self.account_id = account_id
        self.account_number = account_number

        self.client = None

    def get_creds(self, primary_value, sort_value):
        response = self.table.get_item(Key={'PK': primary_value, 'SK': sort_value})
        return {
            'access_key_id': response['Item']['AccessKeyId'],
            'access_key_secret': response['Item']['AccessKeySecret'],
        }

    def create_iam_client(self, primary_value, sort_value):
        # Step 1: Get Credentials From DB
        creds = self.get_creds(
            primary_value=primary_value,
            sort_value=sort_value,
        )

        # Step 2: Create Session and Client
        session = boto3.Session(
            aws_access_key_id=creds['access_key_id'],
            aws_secret_access_key=creds['access_key_secret'],
        )
        self.client = session.client('iam')

    def main(self):
        self.create_iam_client(
            primary_value=f"CLOUD#AWS{self.account_number}",
            sort_value=f"#PROFILE#AWS{self.account_number}",
        )

        # Step 1: Get List of Users
        response = self.client.list_users()
        print(response)

        # Step 2: Add Users to Table If they Don't Exist
        user_names = [user['UserName'] for user in response['Users']]
        for user in user_names:
            result = self.check_if_user_exists(
                name = user,
                company_id = self.company_id,
                account_id = self.account_id,
            )
            if result:
                print(result)
            else:
                create_user(name=user, account_id=self.account_id, company_id=self.company_id)

        # Step 3: Return User Names
        return user_names

    def check_if_user_exists(self, name, company_id, account_id):
        current_users = self.get_users_per_account(company_id=company_id, account_id=account_id)
        for user in current_users:
            if name == user['name']:
                return user['user_id']
        return False

    def get_users_per_account(self, company_id, account_id):
        PK = f"Company#{company_id}"
        SK_Prefix = f"#CloudAcct#{account_id}#CloudUser#"
        response = self.table.query(KeyConditionExpression=Key('PK').eq(PK) & Key('SK').begins_with(SK_Prefix))
        return response['Items']

    def create_user(self, name, account_id, company_id):
        user_id = str(uuid.uuid4())
        item = {
            'name' : name,
            'user_id' : user_id,
            'account_id' : account_id,
        }
        item['PK'] = f"Company#{company_id}"
        item['SK'] = f"#CloudAcct#{account_id}#CloudUser#{user_id}"
        self.table.put_item(Item=item)
        return user_id
