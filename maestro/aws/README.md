# cf_stack

Example usage:

python cf_stack.py --update-stack teststackname --stack-params TablePrefix=Test ReadCapacityUnits=1 WriteCapacityUnits=1 --template-body file:///path/to/cfn_template.yaml --profile dev --regions us-east-1

