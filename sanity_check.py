from typing import List, Union
from db import *

import os
import time
import requests
import json
import logging

from concurrent.futures import ThreadPoolExecutor
from functools import partial

from tqdm import tqdm

log_file = "danbooru.log"




class CachedRequest:
    """
    Wrapper for requests to cache get method
    This is for avoiding rate limiting
    """
    def __init__(self, cache_file="cache.jsonl", proxy_handler=None):
        self.cache_file = cache_file
        self.cache = {}
        self.load_cache()
        self.proxy_handler : ProxyHandler = proxy_handler
    
    def load_cache(self):
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        self.cache[data["url"]] = data["response"]
                    except Exception as e:
                        logging.exception("Error loading cache: {}, skipping line".format(e))
    def get(self, url):
        if url in self.cache:
            return self.cache[url]
        else:
            if self.proxy_handler is not None:
                handler = self.proxy_handler.get()
                #print("Using proxy {}".format(handler))
                if handler is None:
                    r = requests.get(url) # no proxy
                r = requests.get(url, proxies={"http": handler, "https": handler}, timeout=10)
            else:
                r = requests.get(url)
            # if not 200, remove proxy first
            #if r.status_code != 200:
                #if self.proxy_handler is not None:
                    #self.proxy_handler.remove(proxy=r.proxies["http"])
                    #self.proxy_handler.refresh_proxies()
            r.raise_for_status()
            to_json = {"url": url, "response": r.json()}
            # validate, check "id" key
            if "id" not in to_json["response"]:
                raise ValueError("Invalid response: {}".format(to_json["response"]))
            self.cache[url] = to_json["response"]
            with open(self.cache_file, "a") as f:
                f.write(json.dumps(to_json) + "\n")
            return to_json["response"]

class ProxyHandler:
    """
    Wrapper for proxies
    """
    def __init__(self, proxy_file_raw_addr = r""):
        self.proxy_list_raw_addr = proxy_file_raw_addr
        self.proxies = {}
        self.failed_proxies = set()
        self.chosen_index = 0
        self.load_proxies()
        self.last_refresh_time = time.time()
        self.refresh_interval = 10800 # 3 hours
    
    def refresh_everything(self):
        if time.time() - self.last_refresh_time > self.refresh_interval:
            self.refresh_proxies()
            self.last_refresh_time = time.time()

    def load_proxies(self,proxy_type="http"):
        r = requests.get(self.proxy_list_raw_addr.replace("*",proxy_type))
        r.raise_for_status()
        self.proxies[proxy_type] = r.text.split("\n")
        # count
        for i in range(len(self.proxies[proxy_type])):
            if self.proxies[proxy_type][i].count(":") > 1:
                self.proxies[proxy_type][i] = self.proxies[proxy_type][i].rsplit(":",1)[0]
        # remove failed proxies
        self.proxies[proxy_type] = [proxy for proxy in self.proxies[proxy_type] if proxy not in self.failed_proxies]
        print("Testing {} proxies".format(len(self.proxies[proxy_type])))
        #self.test_proxy(proxy_type)
        print("Tested {} proxies".format(len(self.proxies[proxy_type])))
        logging.info("Loaded {} proxies".format(len(self.proxies[proxy_type])))
    def refresh_proxies(self,proxy_type="http"):
        self.load_proxies(proxy_type)
    def get(self, proxy_type="http"):
        self.refresh_everything()
        if self.chosen_index >= len(self.proxies[proxy_type]):
            self.chosen_index = 0
        proxy = self.proxies[proxy_type][self.chosen_index]
        self.chosen_index += 1
        return proxy
    def remove(self, proxy_type="http", proxy=None):
        self.proxies[proxy_type].remove(proxy)
        #self.failed_proxies.add(proxy)
    def test_proxy(self, proxy_type="http", threads=-1):
        if threads == -1:
            threads = len(self.proxies[proxy_type])
            print("Using {} threads".format(threads))
        futures = {}
        proxy_pbar = tqdm(total=len(self.proxies[proxy_type]), desc="Testing proxies")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for proxy in self.proxies[proxy_type]:
                future = executor.submit(self.test_proxy_single, proxy, pbar=proxy_pbar)
                futures[future] = proxy
        success_list = []
        for future in futures:
            if future.result():
                success_list.append(futures[future])
        # finally, replace proxies with success_list
        self.proxies[proxy_type] = success_list
    def test_proxy_single(self, proxy, pbar=None):
        try:
            r = requests.get("https://danbooru.donmai.us/posts/1.json", proxies={"http": proxy, "https": proxy}, timeout=10)
            r.raise_for_status()
        except Exception as e:
            self.failed_proxies.add(proxy)
            return False
        finally:
            if pbar is not None:
                pbar.update(1)
        return True
class DifferenceCache:
    """
    Wrapper for caching differences
    If calculated difference exists, we will use it instead of calculating it again
    """
    def __init__(self, cache_file="difference_cache.jsonl"):
        self.cache_file = cache_file
        self.cache = {}
        self.load_cache()
    
    def load_cache(self):
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        self.cache[data["id"]] = data["difference"]
                    except Exception as e:
                        logging.exception("Error loading cache: {}, skipping line".format(e))
    def get(self, post_id):
        # if tuple, unpack
        if isinstance(post_id, tuple):
            assert len(post_id) == 1, "post_id tuple must be of length 1"
            post_id = post_id[0]
        if post_id in self.cache:
            return self.cache[post_id]
        else:
            difference = compare_info(post_id)
            to_json = {"id": post_id, "difference": difference}
            self.cache[post_id] = to_json["difference"]
            with open(self.cache_file, "a") as f:
                f.write(json.dumps(to_json) + "\n")
            return to_json["difference"]
    def contains(self, post_id):
        return post_id in self.cache
    
class PostPatchStateCache:
    """
    Wrapper for caching post patch states
    Returns True if post is patched, False if not patched
    """
    def __init__(self, cache_file="post_patch_state_cache.jsonl"):
        self.cache_file = cache_file
        self.cache = {}
        self.load_cache()
    
    def load_cache(self):
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        self.cache[data["id"]] = data["state"]
                    except Exception as e:
                        logging.exception("Error loading cache: {}, skipping line".format(e))
    def get(self, post_id):
        return post_id in self.cache
    
    def set(self, post_id, state:bool=True):
        self.cache[post_id] = state
        to_json = {"id": post_id, "state": state}
        with open(self.cache_file, "a") as f:
            f.write(json.dumps(to_json) + "\n")
        return to_json["state"]

class TagCreationCache:
    """
    Wrapper for caching tag creation
    """
    def __init__(self, cache_file="tag_creation_cache.jsonl"):
        self.cache_file = cache_file
        self.cache = {}
        self.load_cache()
    
    def load_cache(self):
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        self.cache[data["id"]] = {"tag_name": data["tag_id"], "tag_context": data["tag_name"]}
                    except Exception as e:
                        logging.exception("Error loading cache: {}, skipping line".format(e))
    def init_tags(self):
        """
        Initialize tags
        """
        for tag_id in self.cache:
            tag = Tag.get_or_none(Tag.id == tag_id)
            if tag is None:
                tag = Tag.create(id=tag_id, name=self.cache[tag_id]["tag_name"], type=self.cache[tag_id]["tag_context"], popularity=-1)
                logging.info("Created tag {} with id {}".format(self.cache[tag_id]["tag_name"], tag.id))
    
    def set(self, tag_id, tag_name, tag_context):
        self.cache[tag_id] = {"tag_name": tag_name, "tag_context": tag_context}
        to_json = {"id": tag_id, "tag_name": tag_name, "tag_context": tag_context}
        with open(self.cache_file, "a") as f:
            f.write(json.dumps(to_json) + "\n")
        return to_json

requests_cache = None
rating_dict = {"s": "sensitive", "q": "questionable", "e": "explicit", "g": "general"}

difference_database = None

patched_posts = PostPatchStateCache()

tag_creation_cache = TagCreationCache()
tag_creation_cache.init_tags()

def convert_tag_ids_to_names(tag_ids: Union[int, Tag, List[Union[int, Tag]]]) -> Union[str, List[str]]:
    """
    Convert tag ids to tag names
    """
    if isinstance(tag_ids, int):
        return Tag.get_by_id(tag_ids).name
    elif isinstance(tag_ids, Tag):
        return tag_ids.name
    elif isinstance(tag_ids, list):
        return [convert_tag_ids_to_names(tag_id) for tag_id in tag_ids]
    else:
        raise TypeError(f"tag_ids must be int, Tag or List[int, Tag] but got {type(tag_ids)}")

def create_tag(string:str, tag_context="general"):
    """Create a tag in the database"""
    tag = Tag.get_or_none(Tag.name == string)
    if tag is None:
        tag = Tag.create(name=string,type=tag_context,popularity=-1)
        logging.info("Created tag {} with id {}".format(string,tag.id))
        tag_creation_cache.set(tag.id, tag_name=string, tag_context=tag_context)
    return tag

def convert_string_to_tag_ids(tag_names: Union[str, List[str]], context="general") -> Union[int, List[int]]:
    """
    Convert tag names to tag ids
    """
    if isinstance(tag_names, str):
        return create_tag(tag_names,context).id
    elif isinstance(tag_names, list):
        return [convert_string_to_tag_ids(tag_name) for tag_name in tag_names]
    else:
        raise TypeError(f"tag_names must be str or List[str] but got {type(tag_names)}")

def get_id_from_tag(tag:Union[Tag,List[Tag]]):
    if isinstance(tag,Tag):
        return tag.id
    elif isinstance(tag,list):
        return [get_id_from_tag(t) for t in tag]
    else:
        raise TypeError(f"tag must be Tag or List[Tag] but got {type(tag)}")


def check_danbooru_post(post_id,by_id=False):
    url = "https://danbooru.donmai.us/posts/{}.json".format(post_id)
    r = requests_cache.get(url)
    result_dict = {
        "id" : r["id"],
        "file_url" : r["large_file_url"] if "large_file_url" in r else r.get("file_url",None), # use large_file_url if available (for high res images)
        "rating" : rating_dict[r["rating"]],
        "year" : r["created_at"][0:4],
        "score" : r["score"],
        "fav_count" : r["fav_count"],
        "tag_list_general" : r["tag_string_general"].split(" "),
        "tag_list_character" : r["tag_string_character"].split(" "),
        "tag_list_artist" : r["tag_string_artist"].split(" "),
        "tag_list_meta" : r["tag_string_meta"].split(" "),
        "tag_list_copyright" : r["tag_string_copyright"].split(" "),
    }
    if by_id:
        for key in result_dict:
            if "tag_list" not in key:
                continue
            result_dict[key] = convert_string_to_tag_ids(result_dict[key],key.split("_")[2])
    return result_dict

def check_database_post(post_id,by_id=True):
    post = Post.get_or_none(Post.id == post_id)
    if post is None:
        return None
    else:
        result_dict = {
            "id" : post.id,
            "file_url" : post.large_file_url if post.large_file_url is not None else getattr(post,"file_url",None), # use large_file_url if available (for high res images)
            "rating" : rating_dict[post.rating],
            "year" : post.created_at[0:4],
            "score" : post.score,
            "fav_count" : post.fav_count,
            "tag_list_general" : get_id_from_tag(post.tag_list_general),
            "tag_list_character" : get_id_from_tag(post.tag_list_character),
            "tag_list_artist" : get_id_from_tag(post.tag_list_artist),
            "tag_list_meta" : get_id_from_tag(post.tag_list_meta),
            "tag_list_copyright" : get_id_from_tag(post.tag_list_copyright),
        }
        if not by_id:
            for key in result_dict:
                if "tag_list" not in key:
                    continue
                result_dict[key] = convert_tag_ids_to_names(result_dict[key])
        return result_dict

def compare_info(post_id, by_id=False):
    """
    Compare danbooru and database info
    Returns the difference dict, <new info>, <old info>
    """
    difference_dict = {},{}
    assert isinstance(post_id, int), f"post_id must be int but got {type(post_id)} with value {post_id}"
    danbooru_info = check_danbooru_post(post_id,by_id=by_id)
    database_info = check_database_post(post_id,by_id=by_id)
    if database_info is None:
        return None, danbooru_info
    for key in danbooru_info:
        # check "tag_list" keys
        if "tag_list" in key:
            if set(danbooru_info[key]) != set(database_info[key]):
                difference_dict[0][key] = set(danbooru_info[key]) - set(database_info[key])
                difference_dict[1][key] = set(database_info[key]) - set(danbooru_info[key])
                # ignore meta tags
                difference_dict[0][key] = [tag for tag in difference_dict[0][key] if not should_ignore_tag(tag)]
                difference_dict[1][key] = [tag for tag in difference_dict[1][key] if not should_ignore_tag(tag)]
        else:
            # update values
            if key == "file_url":
                # check incoming url is valid
                if not danbooru_info[key]:
                    continue
            if danbooru_info[key] != database_info[key]:
                difference_dict[0][key] = database_info[key]
                # we only need to update the database from danbooru
    return difference_dict

from functools import cache
@cache
def should_ignore_tag(tag_id):
    """
    Check if a tag should be ignored
    """
    if isinstance(tag_id, str):
        return "bad" in tag_id and "id" in tag_id # ignore bad_*_id tags
    tag = Tag.get_by_id(tag_id)
    if tag is None:
        return False
    return "bad" in tag.name and "id" in tag.name # ignore bad_*_id tags
import threading
from queue import Queue, Empty
queue = Queue()
event = threading.Event()
pbar = None
def threaded_executor():
    global pbar
    while True:
        try:
            task = queue.get(timeout=0.1)
            task()
            logging.info("Transaction complete")
            if pbar is not None:
                pbar.update(1)
        except Empty:
            if event.is_set():
                logging.info("Thread exiting")
                break
            else:
                logging.debug("Thread sleeping")
                continue
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                logging.info("Thread exiting")
                break
            logging.exception("Error in thread: {}".format(e))
            continue
# if thread is already defined, don't create a new one
if "thread" not in globals():
    thread = threading.Thread(target=threaded_executor)
    thread.start()

def refresh_thread_and_event():
    """
    Refresh the thread and event
    """
    global thread, event
    event.set()
    thread.join()
    event.clear()
    thread = threading.Thread(target=threaded_executor)
    thread.start()

rate_limit_event = threading.Event()
previous_time_sleeped = 2
def handle_rate_limit():
    """
    Handle rate limit
    """
    global rate_limit_event
    if not rate_limit_event.is_set():
        return
    previous_time_sleeped *= 2
    logging.info(f"Rate limit reached, sleeping for {previous_time_sleeped} seconds")
    time.sleep(previous_time_sleeped)
    rate_limit_event.clear()

def get_tags_pair(tag_ids_before, tag_ids_after):
    """
    Yields tag pairs which has 1-difference
    """
    for tag_id in tag_ids_before:
        if tag_id - 1 in tag_ids_after:
            yield tag_id, tag_id - 1
        elif tag_id + 1 in tag_ids_after:
            yield tag_id, tag_id + 1

def patch_differences(id, before, after, submit=True):
    """
    Patch the differences between before and after
    """
    post_by_id = Post.get_by_id(id)
    if post_by_id is None:
        logging.warning(f"Post {id} does not exist, patch failed")
        return
    for key in before:
        if "tag_list" in key:
            # update tags
            tags_list: List[Tag] = getattr(post_by_id, key)
            for tag_id_before, tag_id_after in get_tags_pair(before[key], after[key]):
                tag_before = Tag.get_by_id(tag_id_before) if isinstance(tag_id_before, int) else create_tag(tag_id_before, key.split("_")[2])
                tag_after = Tag.get_by_id(tag_id_after) if isinstance(tag_id_after, int) else create_tag(tag_id_after, key.split("_")[2])
                if tag_before is None or tag_after is None:
                    logging.warning(f"Tag {tag_id_before} or {tag_id_after} does not exist, patch failed for post {id}")
                    continue
                tags_list.remove(tag_before)
                tags_list.append(tag_after)
                logging.info(f"Tag {tag_id_before} replaced by {tag_id_after} for post {id}")
        else:
            # update values
            setattr(post_by_id, key, after[key])
            logging.info(f"Value {key} updated for post {id}")
    # send transaction to queue
    if submit:
        queue.put(lambda: post_by_id.save() and patched_posts.set(id))
        logging.info(f"Transaction saved for post {id}, queue size: {queue.qsize()}")
    else:
        logging.info(f"Transaction not saved for post {id}, cached for further use")
def patch_differences_auto(id, submit=True, retry_count=100):
    """
    Automatically patch the differences between before and after
    """
    # if id is tuple, unpack it
    if isinstance(id, tuple):
        id = id[0]
    if isinstance(id, Tag):
        id = id.id
    #logging.info(f"Checking post {id}")
    handle_rate_limit()
    for _ in range(retry_count):
        try:
            difference_dict = difference_database.get(id)
            break
        except Exception as e:
            # check 429 error
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                rate_limit_event.set()
            #logging.exception(f"Error in post {id}: {e}")
            continue
    global pbar
    if pbar is not None:
        pbar.update(1)
    if difference_dict is None:
        logging.warning(f"Post {id} does not exist, patch failed")
        return
    elif len(difference_dict[0]) == 0:
        logging.info(f"Post {id} is up to date")
        return
    if submit:
        patch_differences(id, difference_dict[1], difference_dict[0], submit=submit)
    else:
        logging.info(f"Post {id} had differences, but not submitted")



def patch_differences_auto_multi(ids, threads=4, submit=True, retry_count=5, total=None):
    """
    Automatically patch the differences between before and after
    """
    refresh_thread_and_event()
    print(f"Starting {threads} threads")
    with ThreadPoolExecutor(max_workers=threads) as executor:
        global pbar
        pbar = tqdm(total=len(ids) if total is None else total)
        submit_pbar = tqdm(ids)
        for id in submit_pbar:
            if isinstance(id, tuple):
                id = id[0]
            if patched_posts.get(id):
                logging.debug(f"Post {id} already patched, skipping")
                pbar.update(1)
                continue
            elif not submit and difference_database.contains(id):
                logging.debug(f"Post {id} already cached, skipping")
                pbar.update(1)
                continue
            executor.submit(partial(patch_differences_auto, id, submit=submit, retry_count=retry_count))
        event.set()

import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sanity check for danbooru database')
    # usage : python sanity_check.py --threads 400 --retry 4000 --start-idx 2000000 --end-idx 4000000 --proxy --logging-file danbooru-proxy.log --save-file difference_cache_proxy.jsonl --proxy-address //*.txt --requests-cache cache_proxy.jsonl
    parser.add_argument('--threads', type=int, default=5, help='Number of threads to use')
    parser.add_argument('--submit', action="store_true", help='Submit the changes to the database')
    parser.add_argument('--retry', type=int, default=5, help='Number of retries for each post')
    parser.add_argument('--start-idx', type=int, default=0, help='Start index for posts')
    parser.add_argument('--end-idx', type=int, default=-1, help='End index for posts')
    parser.add_argument('--all', action="store_true", help='Check all posts')
    parser.add_argument('--proxy', action="store_true", help='Use proxy')
    parser.add_argument('--proxy-address', type=str, default=r"", help='Proxy text file (raw) address')
    parser.add_argument('--logging-file', type=str, default=log_file, help='Logging file')
    parser.add_argument('--save-file', type=str, default="difference_cache.jsonl", help='Difference cache file')
    parser.add_argument('--requests-cache', type=str, default="cache.jsonl", help='Requests cache file')
    args = parser.parse_args()
    logging.basicConfig(filename=args.logging_file, level=logging.INFO)
    difference_database = DifferenceCache(args.save_file)
    requests_cache = CachedRequest(args.requests_cache)
    print(f"Found finished transactions: {len(patched_posts.cache)}")
    print(f"Found cached differences: {len(difference_database.cache)}")
    if args.proxy:
        proxyhandler = ProxyHandler(proxy_file_raw_addr=args.proxy_address)
        # bind
        requests_cache.proxy_handler = proxyhandler
    # lazy iterator for peewee
    all_post_ids = []
    if args.all:
        all_post_ids = Post.select(Post.id).tuples()
    else:
        all_post_ids = Post.select(Post.id).where(Post.id >= args.start_idx)
        if args.end_idx != -1:
            all_post_ids = all_post_ids.where(Post.id <= args.end_idx)
        all_post_ids = all_post_ids.tuples()
    print(f"Found {len(all_post_ids)} posts")
    patch_differences_auto_multi(all_post_ids, threads=args.threads, submit=args.submit, retry_count=args.retry, total=len(all_post_ids))
    logging.info("All posts checked")
    logging.info("Exiting...")
    # set event to stop thread
    event.set()
    thread.join()
    logging.info("Thread joined")
    logging.info("Exiting...")
    if pbar is not None:
        pbar.close()
