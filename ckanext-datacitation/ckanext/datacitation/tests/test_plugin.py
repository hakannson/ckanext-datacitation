'''import ckanext.datacitation.plugin as plugin
import unittest
from  ckan.plugins import toolkit
from mock import patch, MagicMock


class PluginTest(unittest.TestCase):
    def setUp(self):
        self.plugin = plugin.DatacitationPlugin()

    def test_update_config(self):
        with patch.object(toolkit,'add_template_directory',return_value=None) as mock_method:
            self.plugin.update_config({})
            mock_method.assert_called()

        with patch.object(toolkit, 'add_resource', return_value=None) as mock_method:
            self.plugin.update_config({})
            mock_method.assert_called()

    def test_register_backends(self):
        return_val = self.plugin.register_backends()
        print(return_val)
        self.assertIsNotNone(return_val['postgresql'])'''