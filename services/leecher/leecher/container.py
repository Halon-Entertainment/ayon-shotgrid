import sys
import multiprocessing

from .listener import listener_main

import ayon_api


class WorkerException(Exception):
    pass


def start_listener(project_name):
    try:
        return listener_main(project_name)
    except Exception as e:
        raise WorkerException(e)


def container_main():
    ayon_api.init_service()
    projects = ayon_api.get_projects()

    def worker(project):
        project_name = project['name']
        result = start_listener(project_name)
        if result != 0:
            ayon_api.dispatch_event(
                project_name=project_name,
                topic="shotgrid.leecher.failure",
                description="Listener failed",
                store=True,
                finished=True
            )

            raise WorkerException(f"Listener failed for project {project_name}")
        return result

    pool = multiprocessing.Pool(processes=len(projects))

    try:
        for _ in pool.imap_unordered(worker, projects):
            pass
    except WorkerException as e:
        print(str(e))
        pool.terminate()
        pool.join()
        sys.exit(1)

    pool.close()
    pool.join()
    sys.exit(0)
