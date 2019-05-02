import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.datastore.interfaces import IDatastoreBackend
from backend.postgres import VersionedDatastorePostgresqlBackend
from ckanext.datacitation.logic.action import querystore_resolve

class DatacitationPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(IDatastoreBackend)
    plugins.implements(plugins.IRoutes,inherit=True)
    plugins.implements(plugins.IActions)
    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'datacitation')


    # IDatastoreBackend
    def register_backends(self):
        return {
            u'postgresql': VersionedDatastorePostgresqlBackend,
            u'postgres': VersionedDatastorePostgresqlBackend,

        }


    #IRoutes
    def before_map(self,map):
        map.connect('querystore.view', '/querystore/view_query',
                  controller='ckanext.datacitation.controller:QueryStoreController',
                  action='view_history_query')

        map.connect('querystore.dump', '/querystore/dump_history_result_set',
                  controller='ckanext.datacitation.controller:QueryStoreController',
                  action='dump_history_result_set')

        return map

    # IActions
    def get_actions(self):
        actions = {
            'querystore_resolve': querystore_resolve,

        }

        return actions
        



