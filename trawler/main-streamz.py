import shelve
import streamz
import requests
from dask.distributed import Client
from furl import furl

BASE_ZOONIVERSE_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/vnd.api+json; version=1',
}

USERS_DB_LOCATION = 'zoo_users'
PROJECTS_DB_LOCATION = 'zoo_projects'
TALK_DB_LOCATION = 'zoo_projects'


def store_user(user):
    print(f'Storing user {user}')
    key = user['id']
    with shelve.open(USERS_DB_LOCATION) as db:
        db[key] = user
    return key


def store_project(project):
    print(f'Storing project {project}')
    key = project['id']
    with shelve.open(PROJECTS_DB_LOCATION) as db:
        db[key] = project
    return key


def accumulate_results(acc, payload):
    # If asset type is a user, return a pairing of that user with all 
    # currently known projects. If asset type is a project, return
    # a pairing of that project with all currently known users.
    asset_type, asset_value = payload
    unique_store = acc.setdefault(asset_type, set())
    unique_store.add(asset_value)

    if 'projects' in acc and asset_type == 'users':
        return acc, [
            {'projects': p, 'users': asset_value}
            for p in acc['projects']
        ]
    if 'users' in acc and asset_type == 'projects':
        return acc, [
            {'projects': asset_value, 'users': u}
            for u in acc['users']
        ]
    return acc, []


def get_projects(page_size=200):
    '''Iterate over the paginated URL detailing all public projects on the Zooniverse.
    '''
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.api+json; version=1',
    }
    base_url = furl('https://www.zooniverse.org/api')
    next_url = base_url.copy() / 'projects'
    next_url.add({'page_size': page_size})
    while True:
        # TODO: Can we cache this?
        response = requests.get(str(next_url), headers=headers)
        if not response.ok:
            raise ValueError('Could not retrieve project listing from the Zooniverse')
        data = response.json()
        for project in data['projects']:
            yield project
        next_slug = data['meta']['projects'].get('next_href')
        if next_slug is None:
            return
        # Define the next URL to get
        next_url = base_url.copy() / next_slug


def get_users(page_size=200):
    '''Iterate over the paginated URL detailing all public users on the Zooniverse.
    '''
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/vnd.api+json; version=1',
    }
    base_url = furl('https://www.zooniverse.org/api')
    next_url = base_url.copy() / 'users'
    next_url.add({'page_size': page_size})
    while True:
        # TODO: Can we cache this?
        response = requests.get(str(next_url), headers=headers)
        if not response.ok:
            raise ValueError('Could not retrieve user listing from the Zooniverse')
        data = response.json()
        for user in data['users']:
            yield user
        next_slug = data['meta']['users'].get('next_href')
        if next_slug is None:
            return
        # Define the next URL to get
        next_url = base_url.copy() / next_slug


async def get_tasks_worker(name, project_input_stream):
    # TODO Change this to accept an input queue populated by get_projects 
    for p in get_projects():
        project_input_stream.emit(p)


async def get_users_worker(name, users_input_stream):
    # TODO Change this to accept an input queue populated by get_projects 
    for u in get_users():
        users_input_stream.emit(u) 


async def main():
    client = Client()
    print(f'Using dask client {client}')

    user_input_stream = streamz.Stream(asynchronous=True)

    project_input_stream = streamz.Stream(asynchronous=True).map(store_project).map(lambda v: ('projects', v))

    combined_stream = streamz.union(
        user_input_stream.map(store_user).map(lambda v: ('users', v)),
        project_input_stream.map(store_project).map(lambda v: ('projects', v))
    )
    combined_stream.accumulate(accumulate_results, start={}, returns_state=True).flatten().sink(print)

    get_users_worker = asyncio.create_task(get_tasks_worker(f'get-tasks-worker-{0}'))

    for u in users_input[:3]:
        user_input_stream.emit(u)

    for p in project_input[:5]:
        project_input_stream.emit(p)


if __name__ == '__main__':
    main()
