#!/usr/bin/env python

import argparse
import copy
import pyodbc
import requests

from asana2sql.fields import default_fields, default_story_fields
from asana2sql.Project import Project
from asana2sql.Story import Story
from asana2sql.workspace import Workspace
from asana2sql.db_wrapper import DatabaseWrapper
from asana import Client, session

def arg_parser():
    parser = argparse.ArgumentParser()

    # Global options
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
            '--project_id',
            type=int,
            help="Asana project ID.")
    scope.add_argument(
            '--workspace_id',
            type=int,
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
        '--with_subtasks',
        action="store_true",
        default=False,
        help="Fetch and store one level of subtasks as well as top level tasks.")
    
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
            help="Create tables for the project.")

    export_parser = subparsers.add_parser(
            'export',
            help="Export the tasks in the project, "
                 "not deleting deleted tasks from the database.")

    export_parser = subparsers.add_parser(
            'synchronize',
            help="Syncrhonize the tasks in the project with the database.")

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

    if args.command == 'synchronize' and args.table_name and args.workspace_id:
        raise parser.error("To synchronize a workspace, table_name must be omitted; each project requires its own table. Consider using export for workspaces instead.")

    if args.command == 'synchronize' and args.stories_table_name:
        raise parser.error("To synchronize stories, stories_table_name must be omitted; each task requires its own table. Consider using export for stories instead.")

    client = build_asana_client(args)

    db_client = None
    if args.odbc_string:
        print("Connecting to database.")
        db_client = pyodbc.connect(args.odbc_string)

    db_wrapper = DatabaseWrapper(db_client, dump_sql=args.dump_sql, dry=args.dry)

    workspace = Workspace(client, db_wrapper, args)
    project_singleton = Project(client, db_wrapper, workspace, args, default_fields(workspace))
    story_singleton = Story(client, db_wrapper, None, args, default_story_fields(None))

    if args.command == 'create':

        # If we're using one tasks table for all projects' tasks, create it now
        if args.table_name:
            project_singleton.create_table()

        workspace.create_tables()
        # If we're using one stories table for all tasks' stories, create it now
        if args.with_stories and args.stories_table_name:
            story_singleton.create_table()

        if not args.dry:
            db_client.commit()
    elif args.project_id:
        project_main(args, client, db_client, db_wrapper, project_singleton)
    elif args.workspace_id:
        projects = list(client.projects.find_by_workspace(args.workspace_id))
        for asana_project in projects:
            project_id = asana_project.get("id")
            project_args = copy.copy(args)
            vars(project_args)["project_id"] = project_id
            a2s_project = Project(client, db_wrapper, workspace, project_args, default_fields(workspace))
            project_main(project_args, client, db_client, db_wrapper, a2s_project)


def project_main(args, client, db_client, db_wrapper, project):
    if args.command == 'create' and args.table_name is None:
        project.create_table()
    elif args.command == 'export':
        project.export()
    elif args.command == 'synchronize':
        project.synchronize()

    if args.with_stories:
        stories = [Story(client, db_wrapper, task, args, default_story_fields(task)) for task in project.tasks()]
        for story in stories:
            if args.command == 'create' and args.stories_table_name is None:
                story.create_table()
            elif args.command == 'export':
                story.export()
            elif args.command == 'synchronize':
                story.synchronize()

    if not args.dry:
        db_client.commit()

    if args.dump_perf:
        print("Finished `{}' on project {} ({})".format(args.command, project.project_name(), args.project_id))
        print("API Requests: {}".format(client.num_requests))
        print("DB Commands: reads = {}, writes = {}, executed = {}".format(
            db_wrapper.num_reads, db_wrapper.num_writes, db_wrapper.num_executed))

if __name__ == '__main__':
    main()

