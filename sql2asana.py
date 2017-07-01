#!/usr/bin/env python

import argparse
import pyodbc
import requests
import sys

from asana2sql.fields import default_fields, default_story_fields
from asana2sql.Import import ImportUsers, ImportProjects, ImportTasks, ImportTaskParents, ImportProjectMemberships, ImportStories
from asana2sql.Project import Project
from asana2sql.Story import Story
from asana2sql.workspace import Workspace
from asana2sql.db_wrapper import DatabaseWrapper
from asana import Client, session

def arg_parser():
    parser = argparse.ArgumentParser()

    # Global options
    parser.add_argument(
            '--workspace_id',
            type=int,
            required=True,
            help="Asana workspace ID.")

    parser.add_argument(
            '--table_name',
            help=("Name of the SQL table to use for tasks."
                  "If not specified it will be derived from the project name."))

    parser.add_argument(
            '--dump_perf',
            action="store_true",
            default=False,
            help="Print performance information on completion.")

    parser.add_argument(
        '--with_stories',
        action="store_true",
        default=False,
        help="Fetch and store task stories (comments and edit history) as well as task and project details.")

    parser.add_argument("--projects_table_name")
    parser.add_argument("--project_memberships_table_name")
    parser.add_argument("--users_table_name")
    parser.add_argument("--followers_table_name")
    parser.add_argument("--custom_fields_table_name")
    parser.add_argument("--custom_field_enum_values_table_name")
    parser.add_argument("--custom_field_values_table_name")
    parser.add_argument("--stories_table_name")

    # Asana Client options
    asana_args = parser.add_argument_group('Asana Client Options')

    asana_args.add_argument(
            "--access_token",
            required=True,
            help="Asana Personal Access Token for authentication.")

    asana_args.add_argument(
            "--base_url",
            default="https://app.asana.com/api/1.0",
            help="URL of the Asana API server.")

    asana_args.add_argument(
            "--no_verify",
            dest="verify",
            default=True,
            action="store_false",
            help="Turn off HTTPS verification.")

    asana_args.add_argument(
            "--dump_api",
            action="store_true",
            default=False,
            help="Dump API requests to STDOUT")

    # DB options
    db_args = parser.add_argument_group('Database Options')

    db_args.add_argument(
            "--odbc_string",
            help="ODBC connection string.")

    db_args.add_argument(
            "--dump_sql",
            action="store_true",
            default=False,
            help="Dump SQL commands to STDOUT.")

    db_args.add_argument(
            "--dry",
            action="store_true",
            default=False,
            help="Dry run.  Do not actually run any writes to the database.")

    # Commands
    subparsers = parser.add_subparsers(
            title="Commands",
            dest="command")

    create_table_parser = subparsers.add_parser(
            'create',
            help="Create table for the import.")

    map_parser = subparsers.add_parser(
            'map',
            help="Create a mapping from a database object id to an existing object in the new workspace. Map users to automatically reassign tasks in the new workspace.")

    map_parser.add_argument(
            "--type",
            required=True,
            help="The object's type: user, project, task, or story.")

    map_parser.add_argument(
            "--from_id",
            type=int,
            required=True,
            help="The object's id in the old workspace and the database.")

    map_parser.add_argument(
            "--to_id",
            type=int,
            required=True,
            help="The object's id in the old workspace and the database.")

    import_parser = subparsers.add_parser(
            'import',
            help="Import the projects and tasks in the database to a different workspace. Idempotent.")

    import_scope = import_parser.add_mutually_exclusive_group(required=True)
    import_scope.add_argument(
        '--import_all',
        action="store_true",
        default=False,
        help="Import all projects in database to new workspace.")
    import_scope.add_argument(
        '--project_id',
        type=int,
        help="Import one project by id to new workspace.")

    return parser

def build_asana_client(args):
    options = {
        'session': session.AsanaOAuth2Session(
            token={'access_token': args.access_token})}

    if args.base_url:
        options['base_url'] = args.base_url
    if args.verify is not None:
        # urllib3.disable_warnings()
        options['verify'] = args.verify
    if args.dump_api:
        options['dump_api'] = args.dump_api

    return RequestCountingClient(**options);

class RequestCountingClient(Client):
    def __init__(self, dump_api=False, session=None, auth=None, **options):
        Client.__init__(self, session=session, auth=auth, **options)
        self._dump_api = dump_api
        self._num_requests = 0

    @property
    def num_requests(self):
        return self._num_requests

    def request(self, method, path, **options):
        if self._dump_api:
            print("{}: {}".format(method, path))
        self._num_requests += 1
        return Client.request(self, method, path, **options)

def main():
    parser = arg_parser()
    args = parser.parse_args()

    client = build_asana_client(args)

    db_client = None
    if args.odbc_string:
        print("Connecting to database.")
        db_client = pyodbc.connect(args.odbc_string)

    db_wrapper = DatabaseWrapper(db_client, dump_sql=args.dump_sql, dry=args.dry)

    workspace = Workspace(client, db_wrapper, args)
    asana_workspace = client.workspaces.find_by_id(args.workspace_id)
    tasks_singleton = Project(client, db_wrapper, workspace, args, default_fields(workspace))
    stories_singleton = Story(client, db_wrapper, None, args, default_story_fields(None))

    import_users = ImportUsers(client, db_wrapper, args)
    import_projects = ImportProjects(client, db_wrapper, args)
    import_tasks = ImportTasks(client, db_wrapper, args, import_users)
    import_task_parents = ImportTaskParents(client, db_wrapper, args, import_tasks)
    import_project_memberships = ImportProjectMemberships(client, db_wrapper, args, import_projects, import_tasks)
    import_stories = ImportStories(client, db_wrapper, args, import_tasks)

    def commit():
        if not args.dry:
            db_client.commit()

    if args.command == 'create':
        import_users.create_table()
        import_projects.create_table()
        import_tasks.create_table()
        import_task_parents.create_table()
        import_project_memberships.create_table()
        import_stories.create_table()
    elif args.command == 'map':
        if args.type == 'user':
            assert args.from_id == args.to_id, "map: user ids are global, so from_id and to_id should be the same"
            import_users.map(args.from_id, args.to_id)
        elif args.type == 'project':
            import_projects.map(args.from_id, args.to_id)
        elif args.type == 'task':
            import_tasks.map(args.from_id, args.to_id)
        elif args.type == 'story':
            import_stories.map(args.from_id, args.to_id)
        else:
            # idempotent things like task parent / project membership cannot be manually mapped
            raise parser.error("map --type: unsupported type {}".format(args.type))
    elif args.command == 'import':
        import_users.import_once()
        if args.import_all:
            projects = workspace.get_projects()
            tasks = tasks_singleton.db_select_all()
        elif args.project_id:
            projects = [workspace.get_project(args.project_id)]
            tasks = tasks_singleton.db_select_all_in_project(args.project_id)

        for project in projects:
            import_projects.import_once(project)
            commit()
        
        for task in tasks:
            task_id = task["id"]
            import_tasks.import_once(task)
            commit()
            
            memberships = workspace.task_memberships(task_id)
            import_project_memberships.import_once(task, memberships)
            commit()

            stories = stories_singleton.db_select_where("target_id = {}".format(task_id))
            for story in stories:
                import_stories.import_once(task, story)
                commit()

        for task in tasks:
            import_task_parents.import_once(task)
            commit()
           
            
        

            # import tasks in project
            # add project to task if already imported
            # import each task's subtasks
            # TODO ^^
    
    commit()

    if args.dump_perf:
        print("Finished `{}' for workspace {} ({})".format(args.command, asana_workspace.get("name"), asana_workspace.get("id")))
        print("API Requests: {}".format(client.num_requests))
        print("DB Commands: reads = {}, writes = {}, executed = {}".format(
            db_wrapper.num_reads, db_wrapper.num_writes, db_wrapper.num_executed))
        
if __name__ == '__main__':
    main()

