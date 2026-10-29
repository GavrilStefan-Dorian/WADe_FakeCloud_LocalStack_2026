"""Fresh local deployment must not depend on a previously installed demo."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import deploy_local as deploy


class RoleTests(unittest.TestCase):
    def test_fresh_emulator_gets_dedicated_role_and_scoped_policy(self):
        iam = Mock()
        iam.get_role.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchEntity'}}, 'GetRole')
        arn = 'arn:aws:iam::000000000000:role/wade-graduates-lambda'
        iam.create_role.return_value = {'Role': {'Arn': arn}}
        self.assertEqual(deploy.ensure_role(iam), arn)
        trust = json.loads(iam.create_role.call_args.kwargs['AssumeRolePolicyDocument'])
        self.assertEqual(trust['Statement'][0]['Principal'], {'Service': 'lambda.amazonaws.com'})
        policy = json.loads(iam.put_role_policy.call_args.kwargs['PolicyDocument'])
        self.assertEqual(policy['Statement'][1]['Resource'],
                         'arn:aws:s3:::wade-graduates-out/*')

    def test_existing_demo_role_is_reused(self):
        iam = Mock()
        iam.get_role.return_value = {'Role': {'Arn': 'local-role-arn'}}
        self.assertEqual(deploy.ensure_role(iam), 'local-role-arn')
        iam.create_role.assert_not_called()

    def test_permission_error_is_not_mistaken_for_missing_role(self):
        iam = Mock()
        iam.get_role.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'GetRole')
        with self.assertRaises(ClientError):
            deploy.ensure_role(iam)
        iam.create_role.assert_not_called()


if __name__ == '__main__':
    unittest.main()
