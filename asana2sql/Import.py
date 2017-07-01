from asana2sql import util
import asana.error
import itertools

from asana2sql import fields
from asana2sql.Field import SimpleField, SqlType
from asana2sql import workspace
from copy import copy

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
        assert self._validate_new_id(new_id)

        columns = ",".join(field.sql_name for field in self._direct_fields)
        values = ",".join("?" for field in self._direct_fields)
        params = [field.get_data_from_object({
            self._id_field.sql_name: id,
            self._new_id_field.sql_name: new_id
        }) for field in self._direct_fields]
        self._db_client.write(
            INSERT_OR_REPLACE_TEMPLATE.format(
                table_name=self.table_name(),
                columns=columns,
                values=values),
            *params)

    def get_mapping(self, primary_value):
        primary_field = next(filter(lambda fld: fld.primary_key, self._direct_fields))
        other_field = (set([self._id_field, self._new_id_field]) - set([primary_field])).pop()
        sql = SELECT_WHERE_TEMPLATE.format(
            table_name=self.table_name(),
            columns=other_field.sql_name,
            where=""" {} = "{}" """.format(primary_field.sql_name, primary_value))
        results = [row[0] for row in self._db_client.read(sql)]
        assert len(results) < 2, "Found more than one mapping for primary key {}".format(primary_value)
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
    def __init__(self, asana_client, db_client, config, import_users):
        super(ImportTasks, self).__init__("tasks",
                                          asana_client.tasks.find_by_id,
                                          asana_client.tasks.create_in_workspace,
                                          db_client, config)
        self._import_users = import_users

    def import_once(self, task):
        params = copy(task)
        old_assignee = params["assignee_id"]
        if old_assignee:
            new_assignee = self._import_users.get_mapping(old_assignee)
            params["assignee"] = new_assignee or "null"
        else:
            params["assignee"] = "null"

        params["hearted"] = params["num_hearts"] > 0

        if params["due_on"] and not isinstance(params["due_on"], str):
            params["due_on"] = params["due_on"].strftime("%Y-%m-%d")

        if params["due_at"] and not isinstance(params["due_at"], str):
            params["due_at"] = params["due_at"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            del params["due_on"]

        del params["assignee_id"]
        del params["completed_at"]
        del params["created_at"]
        del params["modified_at"]
        del params["num_hearts"]
        del params["parent_id"]
        
        if params["assignee"] == "null":
            del params["assignee_status"]

        self._import_once(self._workspace_id, params)

def get_task_parent_or_throw(asana_client, task_id):
    return asana_client.tasks.find_by_id(task_id, fields="parent").get("parent").get("id")

class ImportTaskParents(ImportSomething):
    def __init__(self, asana_client, db_client, config, import_tasks):
        super(ImportTaskParents, self).__init__("task_parents",
                                                lambda tid: get_task_parent_or_throw(asana_client, tid),
                                                asana_client.tasks.set_parent,
                                                db_client, config)
        self._id_field = SimpleField("new_parent_id", SqlType.INTEGER)
        self._new_id_field = SimpleField("new_id", SqlType.INTEGER, primary_key=True)
        self._direct_fields = [self._id_field, self._new_id_field]
        self._import_tasks = import_tasks

    def import_once(self, task):
        old_parent_id = task["parent_id"]
        if not old_parent_id:
            return

        new_id = self._import_tasks.get_mapping(task["id"])
        if self.get_mapping(new_id):
            return

        new_parent_id = self._import_tasks.get_mapping(old_parent_id)
        assert new_parent_id, "import_parent: has parent, but new parent id not found for old parent id {}".format(old_parent_id)

        self._asana_client_create(new_id, { "parent": new_parent_id })
        self.map(new_parent_id, new_id)

def split_key(key):
    fst, snd = key.split("|")
    return (int(fst), int(snd))

def join_key(fst, snd):
    return "|".join([str(fst), str(snd)])

def get_project_membership_or_throw(asana_client, composite_pt_key):
    pid, tid = split_key(composite_pt_key)
    task = asana_client.tasks.find_by_id(tid, fields="projects")
    return next(filter(lambda proj: proj["id"] == pid, task["projects"]), None).get("id")

class ImportProjectMemberships(ImportSomething):
    def __init__(self, asana_client, db_client, config, import_projects, import_tasks):
        super(ImportProjectMemberships, self).__init__("project_memberships",
                                                       lambda proj_task: get_project_membership_or_throw(asana_client, proj_task),
                                                       asana_client.tasks.add_project,
                                                       db_client, config)
        self._id_field = SimpleField("id", SqlType.STRING, primary_key=True)
        self._new_id_field = SimpleField("new_id", SqlType.STRING)
        self._direct_fields = [self._id_field, self._new_id_field]
        self._import_projects = import_projects
        self._import_tasks = import_tasks

    def import_once(self, task, old_project_ids):
        old_task_id = task["id"]
        old_project_ids = [old_pid for old_pid in old_project_ids if not self.get_mapping(join_key(old_pid, old_task_id))]
        if not old_project_ids:
            return
        
        new_id = self._import_tasks.get_mapping(task["id"])
        for old_pid in old_project_ids:
            new_pid = self._import_projects.get_mapping(old_pid)
            self._asana_client_create(new_id, { "project": new_pid })
            self.map(join_key(old_pid, old_task_id), join_key(new_pid, new_id))
        
class ImportStories(ImportSomething):
    def __init__(self, asana_client, db_client, config, import_tasks):
        super(ImportStories, self).__init__("stories",
                                            asana_client.stories.find_by_id,
                                            asana_client.stories.create_on_task,
                                            db_client, config)
        self._import_tasks = import_tasks

    def import_once(self, task, story):
        if story['type'] == 'system':
            return

        new_task_id = self._import_tasks.get_mapping(task["id"])
        params = copy(story)
        del params["created_at"]
        
        self._import_once(new_task_id, params)
