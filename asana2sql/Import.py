from asana2sql import util
import asana.error
import itertools

from asana2sql import fields
from asana2sql.Field import SimpleField, SqlType
from asana2sql import workspace

CREATE_TABLE_TEMPLATE = (
        """CREATE TABLE IF NOT EXISTS "{table_name}" ({columns});""")

INSERT_OR_REPLACE_TEMPLATE = (
        """INSERT OR REPLACE INTO "{table_name}" ({columns}) VALUES ({values});""")

SELECT_TEMPLATE = (
        """SELECT {columns} FROM "{table_name}";""")

SELECT_WHERE_TEMPLATE = (
        """SELECT {columns} FROM "{table_name}" WHERE {where};""")

DELETE_TEMPLATE = (
        """DELETE FROM "{table_name}" WHERE {id_column} = ?;""")

class ImportSomething(object):
    def __init__(self, thing_name, asana_client_get, asana_client_create, db_client, config):
        self._thing_name = thing_name
        self._asana_client_get = asana_client_get
        self._asana_client_create = asana_client_create
        self._db_client = db_client
        self._config = config
        self._workspace_id = self._config.workspace_id

        self._id_field = SimpleField("id", SqlType.INTEGER, primary_key=True)
        self._new_id_field = SimpleField("new_id", SqlType.INTEGER)
        self._direct_fields = [self._id_field, self._new_id_field]


    def table_name(self):
        return util.sql_safe_name("import {} {}".format(self._workspace_id, self._thing_name))

    def create_table(self):
        sql = CREATE_TABLE_TEMPLATE.format(
            table_name=self.table_name(),
            columns=",".join([field.field_definition_sql() for field in self._direct_fields]))
        self._db_client.write(sql)

    def map(self, id, new_id):
        self._validate_new_id(new_id)

        columns = ",".join(field.sql_name for field in self._direct_fields)
        values = ",".join("?" for field in self._direct_fields)
        params = [field.get_data_from_object({"id": id, "new_id": new_id}) for field in self._direct_fields]
        self._db_client.write(
            INSERT_OR_REPLACE_TEMPLATE.format(
                table_name=self.table_name(),
                columns=columns,
                values=values),
            *params)

    def get_mapping(self, id):
        sql = SELECT_WHERE_TEMPLATE.format(
            table_name=self.table_name(),
            columns=self._new_id_field.sql_name,
            where="{} = {}".format(self._id_field.sql_name, id))
        results = [row[0] for row in self._db_client.read(sql)]
        assert len(results) < 2, "Found more than one mapping for primary key {}".format(id)
        if results:
            return results[0]
        else:
            return None
            
    def _validate_new_id(self, new_id):
        return self._asana_client_get(new_id) is not None # will throw if not found

    def _import_once(self, location_id, data):
        id = data["id"]
        if self.get_mapping(id):
            return
        
        result = self._asana_client_create(location_id, data)
        new_id = result.get("id")
        self.map(id, new_id)
        return new_id

class ImportUsers(ImportSomething):
    def __init__(self, asana_client, db_client, config):
        super(ImportUsers, self).__init__("users",
                                          asana_client.users.find_by_id,
                                          None,
                                          db_client, config)
        self._me = asana_client.users.me()

    def import_once(self):
        me_id = self._me.get("id")
        self.map(me_id, me_id)

class ImportProjects(ImportSomething):
    def __init__(self, asana_client, db_client, config):
        super(ImportProjects, self).__init__("projects",
                                             asana_client.projects.find_by_id,
                                             asana_client.projects.create_in_workspace,
                                             db_client, config)

    def import_once(self, project):
        self._import_once(self._workspace_id, project)

class ImportTasks(ImportSomething):
    def __init__(self, asana_client, db_client, config):
        super(ImportTasks, self).__init__("tasks",
                                          asana_client.tasks.find_by_id,
                                          asana_client.tasks.create_in_workspace,
                                          db_client, config)

class ImportStories(ImportSomething):
    def __init__(self, asana_client, db_client, config):
        super(ImportStories, self).__init__("stories",
                                            asana_client.stories.find_by_id,
                                            asana_client.stories.create_on_task, # TODO set task creation id
                                            db_client, config)
