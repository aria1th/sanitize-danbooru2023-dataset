import os
import json
import requests
import glob
from tqdm import tqdm
from utils.proxyhandler import ProxyHandler

def yield_posts(file_dir=r"G:\database\post", from_id=0, end_id=7110548):
    """
    Yields the posts
    """
    # using listdir instead of glob because glob is slow
    files = []
    # walk through all files
    for root, dirs, filenames in os.walk(file_dir):
        for filename in filenames:
            if "_" not in filename:
                continue
            # 0_19.jsonl -> 0, 19
            start_id, end_id = filename.split(".")[0].split("_")
            start_id = int(start_id)
            end_id = int(end_id)
            if start_id > end_id:
                continue
            if end_id < from_id:
                continue
            files.append(os.path.join(root, filename))
    print(f"Total {len(files)} files")
    for file in files:
        with open(file, 'r') as f:
            yield from f.readlines()

def download_post(post_dict, proxyhandler:ProxyHandler, pbar=None, no_split=False, save_location="G:/danbooru2023-c/", split_size=1000000, max_retry=10):
    post_id = post_dict['id']
    ext = post_dict['file_ext']
    download_target = post_dict.get("large_file_url", post_dict.get("file_url"))
    save_path = save_location +f"{post_id % 100} /"+ f"{post_id}.{ext}"
    if not os.path.exists(save_location +f"{post_id % 100} /"):
        os.makedirs(save_location +f"{post_id % 100} /")
    # if url contains file extension, use that
    if download_target and "." in download_target:
        ext = download_target.split(".")[-1]
    # skip video files
    if ext in ["webm", "mp4", "mov", "avi"]:
        return
    if not download_target:
        #print(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
        return
    for i in range(max_retry):
        try:
            filesize = proxyhandler.filesize(download_target)
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise e
            print(f"Exception: {e} when getting filesize of {post_id}, retrying {i}/{max_retry}")
            filesize = None
        if filesize is not None:
            break
    if filesize is None:
        print(f"Error: {post_id} has no filesize after {max_retry} retries")
        return

    if os.path.exists(save_path):
        # check file size
        if os.path.getsize(save_path) != filesize:
            print(f"Error: {post_id} had different file size saved, expected {filesize}, got {os.path.getsize(save_path)}")
            os.remove(save_path)
        else:
            if pbar is not None:
                pbar.update(1)
            return
    if no_split:
        for i in range(max_retry):
            try:
                file_response = proxyhandler.get(download_target)
                if file_response and file_response.status_code == 200:
                    break
                else:
                    print(f"Error: {post_id}, {file_response}")
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Exception: {e} when downloading {post_id}, retrying {i}/{max_retry}")
                pass
        filesize = file_response.headers.get('Content-Length')
        content = file_response.content
        # compare file size
        if int(filesize) != len(content):
            print(f"Error: {post_id} had different file size when downloading (no split), expected {filesize}, got {len(content)}")
            return
            # save file
        with open(save_path, 'wb') as f:
            f.write(content)
    else:
        datas = [] # max 1MB per request
        if filesize is None:
            print(f"Error: {post_id} has no filesize")
            return
        for i in range(0, filesize, split_size):
            datas.append((i, min(filesize, i + split_size)))
        # download
        current_filesize = os.path.getsize(save_path) if os.path.exists(save_path) else 0
        if current_filesize:
            print(f"Resuming {post_id} from {current_filesize}, to {filesize}")
        with open(save_path, 'wb') as f:
            for data in datas:
                if data[0] < current_filesize:
                    continue
                for i in range(max_retry):
                    try:
                        file_response = proxyhandler.get_filepart(download_target, data[0], data[1] - 1)
                        if file_response and file_response.status_code == 200:
                            break
                        else:
                            print(f"Error: {post_id}, {file_response.status_code if file_response else None}")
                    except Exception as e:
                        if isinstance(e, KeyboardInterrupt):
                            raise e
                        print(f"Exception: {e} when downloading {post_id} {data[0]}-{data[1]}, retrying {i}/{max_retry}")
                if file_response is None or file_response.status_code != 200:
                    print(f"Error: {post_id}, {file_response.status_code}")
                    return
                # check file size
                if int(file_response.headers.get('Content-Length')) != data[1] - data[0]:
                    print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {file_response.headers.get('Content-Length')}")
                    return
                f.write(file_response.content)
        # compare file size
        if os.path.getsize(save_path) != filesize:
            print(f"Error: {post_id} had different file size after downloading, expected {filesize}, got {os.path.getsize(save_path)}")
            os.remove(save_path)
            return
    if pbar is not None:
        pbar.update(1)

def test():
    post_getter = yield_posts(file=file)
    # skip 1000 posts
    post = next(post_getter)
    file_url = post.get("large_file_url", post.get("file_url"))
    file_size = proxyhandler.filesize(file_url)
    print(f"File Size: {file_size}")
    if not os.path.exists(save_location):
        os.makedirs(save_location)
    download_post(post, proxyhandler, no_split=False)
    raise Exception("Stop")
if __name__ == '__main__':
    proxy_list_file = r"G:\database\proxy_list.txt"
    save_location = "G:/danbooru2023-c/"
    proxyhandler = ProxyHandler(proxy_list_file, wait_time=0.1, timeouts=20,proxy_auth="user:password_notdefault")
    proxyhandler.check()
    from concurrent.futures import ThreadPoolExecutor
    # test
    with ThreadPoolExecutor(max_workers=80) as executor:
        pbar = tqdm(total=-6400000 +7110548)
        for post in yield_posts(from_id=6400000, end_id=7110548):
            try:
                post = json.loads(post)
            except:
                print(f"Error: {post}")
                continue
            #download_post(post, proxyhandler, pbar=pbar, no_split=False, save_location=save_location,split_size=1000000)
            executor.submit(download_post, post, proxyhandler, pbar=pbar, no_split=False, save_location=save_location,split_size=1000000)
