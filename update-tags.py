#https://danbooru.donmai.us/tags.json?page=1&limit=100&search[id_gt]=0&search[id_lt]=100

from typing import List
import requests
import os
from functools import cache
import json
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils.proxyhandler import ProxyHandler

handler = ProxyHandler("ips.txt", port=80, wait_time=0.12, timeouts=15, proxy_auth="user:password_notdefault")
handler.check()
print(f"Proxy Handler Checked, total {len(handler.proxy_list)} proxies")
filelock = Lock()
# faster
PER_REQUEST_POSTS = 100
post_ids = set()
@cache
def split_query(start, end) -> List[str]:
    """
    Returns the list of queries to be made
    """
    lists = []
    skipped = 0
    end = end + PER_REQUEST_POSTS if end % PER_REQUEST_POSTS != 0 else end
    for i in tqdm(range(start, end, PER_REQUEST_POSTS)):
        if i in post_ids:
            skipped += 1
            #print(f"Skipping {i}")
            continue
        lists.append(get_query_bulk(i))
    print(f"Skipped {skipped} queries")
    return lists
def get_query_bulk(index):
    """
    Returns the query link that contains the index
    """
    start_idx = index - index % PER_REQUEST_POSTS
    end_idx = start_idx + PER_REQUEST_POSTS
    query = rf"https://danbooru.donmai.us/tags.json?page=1&limit={PER_REQUEST_POSTS}&search[id_ge]={start_idx}&search[id_lt]={end_idx}"
    print(f"Query: {query}")
    return query

def get_response(url):
    """
    Returns the response of the url
    """
    try:
        response = handler.get_response(url)
        return response
    except Exception as e:
        print(f"Exception: {e}")
        return None
total_posts = 0
def write_to_file(data, post_file='tag.jsonl'):
    """
    Writes the data to the file
    """
    global pbar, total_posts
    skipped = 0
    # if no directory, create directory
    if not os.path.exists(os.path.dirname(post_file)):
        os.makedirs(os.path.dirname(post_file), exist_ok=True)
    try:
        if os.path.exists(post_file):
            return
        with open(post_file, 'w') as f:
            if not isinstance(data, list):
                print(f"Error: {data}")
            total_posts += len(data)
            if len(data) != PER_REQUEST_POSTS:
                print(f"Warning: {len(data)} posts in response, expected {PER_REQUEST_POSTS}")
            print(f"Wrote {total_posts} posts to file")
            for post in data:
                if 'id' not in post:
                    print(f"Error: {post}")
                    continue
                #assert "file_url" in post or "large_file_url" in post, f"Post has no file url: {post['id']} : post {post}" # gold account?
                f.write(json.dumps(post))
                f.write('\n')
                post_ids.add(post['id'])
    except Exception as e:
        print(f"Exception: {e} while writing to file")
def get_posts(query, post_file='tags.jsonl'):
    """
    Gets the posts from the query
    """
    global pbar
    if os.path.exists(post_file):
        pbar.update(1)
        return
    response = get_response(query)
    if response is not None:
        write_to_file(response, post_file=post_file)
    else:
        print(f"Error: {query}")
    pbar.update(1)

def get_posts_threaded(queries, post_file='tags/tag.jsonl'):
    """
    Gets the posts from the queries
    """
    def get_post_range(query):
        """
        Returns the range of the query
        """
        return int(query.split("id_ge]=")[1].split("&")[0]), int(query.split("id_lt]=")[1])
    def get_filename_for_query(query):
        """
        Returns the filename for the query
        """
        start, end = get_post_range(query)
        # create subdir by millions
        return f"tags/{start // 1000000}M/{start}_{end}.jsonl"
    global pbar
    with ThreadPoolExecutor(max_workers=len(handler.proxy_list) * 5) as executor:
        futures = [executor.submit(get_posts, query, post_file=get_filename_for_query(query)) for query in queries]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception: {e}")
    #wait until all threads are done
if __name__ == '__main__':
    # test
    post_file = 'tags/tag.jsonl'
    if os.path.exists(post_file):
        _lines = 0
        with open(post_file, 'r') as f:
            for line in f:
                try:
                    post_ids.add(json.loads(line)['id'])
                except:
                    print(f"Error: {line}")
                _lines += 1
        print(f"Total Posts: {len(post_ids)}")
        print(f"Total Lines: {_lines}")
    queries = split_query(1, 2068075)
    pbar = tqdm(total=len(queries))
    get_posts_threaded(queries, post_file=post_file)
