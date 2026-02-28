'''
How to deploy

$ prefect deploy /app/flows/github_stars.py:github_stars \
  --name github-stars-prod \
  --pool my-pool \
  --cron "*/10 * * * *"
'''

from prefect import flow, task
from typing import List
import httpx


@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    for repo in repos:
        get_stars(repo)

# run the flow!
if __name__=="__main__":
    github_stars.serve(name="first-deployment", cron="* * * * *")
