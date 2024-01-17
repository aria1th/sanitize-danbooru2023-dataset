from typing import List
import requests
import os
from functools import cache
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

last_commited = time.time()
def wait_until_commit():
    global last_commited
    while time.time() - last_commited < 0.1:
        time.sleep(0.01)
    last_commited = time.time()

# faster
PER_REQUEST_POSTS = 100

@cache
def split_query(start, end) -> List[str]:
    """
    Returns the list of queries to be made
    """
    # https://danbooru.donmai.us/posts.json?tags=id%3A6000000..6000099&limit=100
    # https://danbooru.donmai.us/posts.json?tags=id%3A{start}..{start+99}&limit={PER_REQUEST_POSTS}
    lists = []
    end = end + PER_REQUEST_POSTS if end % PER_REQUEST_POSTS != 0 else end
    for i in range(start, end, PER_REQUEST_POSTS):
        lists.append(get_query_bulk(i))
    return lists
def get_query_bulk(index):
    """
    Returns the query link that contains the index
    """
    start_idx = index - index % PER_REQUEST_POSTS
    end_idx = start_idx + PER_REQUEST_POSTS - 1
    # no page
    query = f"https://danbooru.donmai.us/posts.json?tags=id%3A{start_idx}..{end_idx}&limit={PER_REQUEST_POSTS}"
    return query
    
# test
queries = split_query(6400000, 7107810)

post_file = 'posts.jsonl'

def get_response(url):
    """
    Returns the response of the url
    """
    try:
        wait_until_commit()
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
        return None

pbar = tqdm(total=len(queries))
def write_to_file(data):
    """
    Writes the data to the file
    """
    global pbar
    with open(post_file, 'a') as f:
        for post in data:
            f.write(json.dumps(post))
            f.write('\n')

def get_posts(query):
    """
    Gets the posts from the query
    """
    global pbar
    response = get_response(query)
    if response is not None:
        write_to_file(response)
    else:
        print(f"Error: {query}")
    pbar.update(1)

def get_posts_threaded(queries):
    """
    Gets the posts from the queries
    """
    global pbar
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_posts, query) for query in queries]
        for future in as_completed(futures):
            future.result()
    pbar.close()
if __name__ == "__main__":
    # get_posts(queries[0])
    get_posts_threaded(queries)
