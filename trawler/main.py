from typing import Optional
from time import time
import shelve
import redo
import asyncio
import requests
import aiohttp
from types import SimpleNamespace
import panoptes_client
from stats_dataclasses import ProjectEventsOverTimeCount

USERS_DB_LOCATION = 'zoo_users'
PROJECTS_DB_LOCATION = 'zoo_projects'
TALK_DB_LOCATION = 'zoo_projects'

# Writing to shelve:
# with shelve.open(DB_NAME) as db:
#     db['key'] = value
# Note that "The shelve module does not support concurrent read/write access to shelved objects."

# We want to retrieve
# - Basic details about each user
# - Basic details about each project
# - How many classifications a user has posted on a project
# - How many talk posts a user has posted on a project
# - How many talk posts have been posted on a project

# Method:
# 1. Start a worker that iterates over all projects using the panoptes client, putting details into a queue to be written to disk
#    - How to cache this?
# 2. Start a worker that iterates over all users using the panoptes client, putting details into a queue to be written to disk
#    - How to cache this?
# 3. Start a worker that keeps a record of all project IDs and user IDs observed, and puts the combinations into a user+project work queue
# 4. Start a "project stats" worker that gets talk + classification stats for each project
# 5. Start a "user+project stats" worker that listens to the user+project work queue and requests worker-specific details on the project

async def http_worker(name, input_queue):
    async with aiohttp.ClientSession() as session:
        while True:
            # Retrieve a work item from the queue
            (url, output_queue) = await input_queue.get()

            # Execute request and trigger callback
            async with session.get(url) as response:
                data = await response.json()
                await output_queue.put((url, data))

            # Notify the queue that the work item has been processed.
            input_queue.task_done()


template_urls = SimpleNamespace(
    # Getting project details
    project = '',
    # Getting user details
    user = '',
    # Getting all comments on a project
    comments_on_project = 'https://stats.zooniverse.org/counts/comment/hour?project_id={project_id}',
    # Getting a user's comments on a project:
    user_comments_on_project = 'https://stats.zooniverse.org/counts/comment/hour?user_id={user_id}&project_id={project_id}',
    # Getting all classifications on a project
    classifications_on_project = 'https://stats.zooniverse.org/counts/classification/hour?project_id={project_id}',
    # Getting a user's classifications on a project:
    user_classifications_on_project = 'https://stats.zooniverse.org/counts/classification/hour?user_id={user_id}&project_id={project_id}',
)


db_names = SimpleNamespace(
    users='users',
    projects='projects',
    users_on_projects='users_on_projects',
)


def get_projects(page_size=200):
    '''Iterate over the paginated URL detailing all public projects on the Zooniverse, triggering.
    '''
    url = 'https://www.zooniverse.org/api/projects'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.api+json; version=1',
    }
    next_url = f'https://www.zooniverse.org/api/projects?page_size={page_size}'
    project_ids = []
    with shelve.open(PROJECTS_DB_LOCATION) as db:
        while True:
            response = requests.get(url, headers=headers)
            if not response.ok:
                raise ValueError('Could not retrieve project listing from the Zooniverse')
            data = response.json()
            for project in data['projects']:
                db[project['id']] = project
            project_ids.extend([p['id'] for p in data['projects']])
            next_url = data['meta']['projects'].get('next_href')
            if not next_url:
                break
    return project_ids


async def main(query_projects: bool=False):
    if query_projects:
        project_ids = redo.retry(get_projects, sleeptime=0.5, jitter=0)
    else:
        with shelve.open(PROJECTS_DB_LOCATION) as db:
            project_ids = list(db.keys())
    print(len(project_id))



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())