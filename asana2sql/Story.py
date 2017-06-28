from asana2sql import util
import asana.error
import itertools

from asana2sql import fields
from asana2sql import workspace

CREATE_TABLE_TEMPLATE = (
        """CREATE TABLE IF NOT EXISTS "{stories_table_name}" ({columns});""")

INSERT_OR_REPLACE_TEMPLATE = (
        """INSERT OR REPLACE INTO "{stories_table_name}" ({columns}) VALUES ({values});""")

SELECT_TEMPLATE = (
        """SELECT {columns} FROM "{stories_table_name}";""")

DELETE_TEMPLATE = (
        """DELETE FROM "{stories_table_name}" WHERE {id_column} = ?;""")

class NoSuchStoryException(Exception):
    def __init__(self, story_id):
        super(NoSuchStoryException, self).__init__(
                "No story with id {}".format(story_id))

class Story(object):
    """Represents a story on Asana.  The class executes commands to bring the
    database into sync with the story data.
    """

    def __init__(self, asana_client, db_client, task, config, fields):
        self._asana_client = asana_client
        self._db_client = db_client
        self._task = task
        self._config = config
        self._direct_fields = []
        self._indirect_fields = []

        self._stories_table_name = self._config.stories_table_name

        self._story_cache = None

        for field in fields:
            self._add_field(field)

    def _stories(self):
        """Fetch all the task's story data from Asana and cache it."""
        if self._story_cache is None:
            self._story_cache = list(
                    self._asana_client.stories.find_by_task(
                        self._task.get("id"), fields=",".join(self._required_fields())))
        return self._story_cache

    def _required_fields(self):
        return set(field_names for field in self._direct_fields + self._indirect_fields
                               for field_names in field.required_fields())

    def stories_table_name(self):
        return util.sql_safe_name(self._stories_table_name if self._stories_table_name else "stories")

    def _add_field(self, field):
        if field.sql_name:
            self._direct_fields.append(field)
        else:
            self._indirect_fields.append(field)

    def create_table(self):
        sql = CREATE_TABLE_TEMPLATE.format(
                stories_table_name=self.stories_table_name(),
                columns=",".join([
                        field.field_definition_sql() for field in self._direct_fields]))
        self._db_client.write(sql)

    def export(self):
        for story in self._stories():
            self.insert_or_replace(story)

    def insert_or_replace(self, story):
        columns = ",".join(field.sql_name for field in self._direct_fields)
        values = ",".join("?" for field in self._direct_fields)
        params = [field.get_data_from_object(story) for field in self._direct_fields]
        self._db_client.write(
                INSERT_OR_REPLACE_TEMPLATE.format(
                    stories_table_name=self.stories_table_name(),
                    columns=columns,
                    values=values),
                *params)

        for field in self._indirect_fields:
            field.get_data_from_object(story)

    def delete(self, task_id):
        id_field = self._id_field()
        self._db_client.write(
                DELETE_TEMPLATE.format(
                    stories_table_name=self.stories_table_name(),
                    id_column=id_field.sql_name),
                task_id)

    def synchronize(self):
        db_story_ids = self.db_story_ids()
        asana_story_ids = self.asana_story_ids()

        ids_to_remove = db_story_ids.difference(asana_story_ids)

        for story in self._stories():
            self.insert_or_replace(story)

        for id_to_remove in ids_to_remove:
            self.delete(id_to_remove)

    def asana_story_ids(self):
        return set(story.get("id") for story in self._stories())

    def _id_field(self):
        return self._direct_fields[0]  # TODO: make the id field special.

    def db_story_ids(self):
        id_field = self._id_field()
        return set(row[0] for row in self._db_client.read(
                SELECT_TEMPLATE.format(
                    stories_table_name=self.stories_table_name(),
                    columns=id_field.sql_name)))

