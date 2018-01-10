#!/usr/bin/env python
import os
import sys
import unittest
sys.path.append(os.path.abspath('.'))

from sqlwrapper import sqlwrapper


class TestCases(unittest.TestCase):

    def tearDown(self):
        keys = ['DB_NAME', 'DB_HOST', 'DB_USERNAME', 'DB_PASSWORD']
        for key in keys:
            if key in sqlwrapper.CONNECTION_DETAILS:
                del sqlwrapper.CONNECTION_DETAILS[key]

            if key in os.environ:
                del os.environ[key]

    def test_get_config_no_config(self):
        self.assertEqual(
            sqlwrapper.get_default_config(),
            sqlwrapper.CONNECTION_DETAILS
        )

    def test_get_config_changed_config(self):
        sqlwrapper.CONNECTION_DETAILS.update({
            'DB_NAME': 'test', 'DB_HOST': 'localhost',
            'DB_USERNAME': 'root', 'DB_PASSWORD': 'PASSWORD'
        })
        config = sqlwrapper.CONNECTION_DETAILS
        config.update({'DBM_TYPE': 'MYSQL'})
        self.assertEqual(config, sqlwrapper.get_default_config())

    def test_get_config_from_sys_env(self):
        tmp_dict = {
            'DB_NAME': 'db',
            'DB_HOST': 'localhost',
            'DB_USERNAME': 'user',
            'DB_PASSWORD': 'pass'
        }
        for key in tmp_dict.keys():
            os.environ[key] = tmp_dict[key]
        tmp_dict.update({'DBM_TYPE': 'MYSQL'})
        self.assertEqual(tmp_dict, sqlwrapper.get_default_config())

    def test_validate_limit_with_negative_index(self):
        self.assertRaises(Exception, sqlwrapper.validate_limits, *(-1, 0))

    def test_validate_limit_with_zero_index(self):
        self.assertTrue(sqlwrapper.validate_limits(0, 1))

    def test_validate_limit_with_negative_limit(self):
        self.assertRaises(Exception, sqlwrapper.validate_limits, *(0, -1))

    def test_validate_limit_with_zero_limit(self):
        self.assertRaises(Exception, sqlwrapper.validate_limits, *(0, 0))

    def test_get_connection(self):
        pass


if __name__ == '__main__':
    # print(p)
    # print(x)
    unittest.main()
