# cf_stack

Example usage:

python cf_stack.py --update-stack teststackname --stack-params TablePrefix=Test ReadCapacityUnits=1 WriteCapacityUnits=1 --template-body file:///path/to/cfn_template.yaml --regions us-east-1 --profile dev

# r53

Example usage:

python r53.py --get --zones testing.example.com blart.example.com --profile dev

python r53.py --exists --zones testing.example.com --records test01 --profile dev

python r53.py --exists --records test01.testing.example.com --profile dev