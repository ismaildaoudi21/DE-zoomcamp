
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("zoomcamp")


github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='github_flow'
    fs = RemoteFileSystem(basepath="s3://my-bucket/folder/")
)

if __name__ == '__main__':
    github_dep.apply()

