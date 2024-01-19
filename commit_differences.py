"""
Reads and commits differences from difference_cache.jsonl to database
Returns number of differences committed
"""

import glob
from random import random, choice
from db import *
import os
import json
from tqdm import tqdm

def is_different(diff_0, diff_1):
    if not diff_0 and not diff_1:
        return False
    return True
    
def get_differences(filepath):
    """
    Reads differences from difference_cache.jsonl
    Returns list of differences
    """
    differences = {}
    with open(filepath, 'r') as file:
        for line in file:
            try:
                loaded_dict = json.loads(line)
                if not loaded_dict['difference']:
                    continue
                if not is_different(loaded_dict['difference'][0], loaded_dict['difference'][1]):
                    continue
                differences[loaded_dict['id']] = loaded_dict['difference']
            except:
                pass
    return differences

def commit_differences(differences):
    """
    Commits differences to database
    Returns number of differences committed
    """
    for ids in tqdm(differences):
        if not is_different(differences[ids][0], differences[ids][1]):
            continue
        commit_difference(ids, differences[ids])
def commit_difference(id, difference):
    """
    Commits difference to database
    """
    if difference:
        post_by_id = Post.get_by_id(id)
        new_dict, old_dict = difference
        for keys in new_dict:
            if keys not in old_dict:
                post_by_id.__setattr__(keys, new_dict[keys])
            else:
                # replace old value with new value
                if "tag" not in keys:
                    print(f"Warning: {keys} is not a tag")
                current_value = post_by_id.__getattribute__(keys)
                target_values_to_remove = old_dict[keys]
                target_values_to_add = new_dict[keys]
                # remove old values
                for values in current_value:
                    name = values.name
                    if name in target_values_to_remove:
                        current_value.remove(values)
                # add new values
                for values in target_values_to_add:
                    # no int values allowed
                    assert isinstance(values, str), f"Error: {values} is not a string"
                    tag = Tag.get_or_none(Tag.name == values)
                    if tag is None:
                        print(f"Warning: {values} is not in database, adding")
                        tag = Tag.create(name=values, type=keys.split("_")[-1], popularity=0)
                    # assert unique, tag should not be iterable
                    assert not isinstance(tag, list), f"Error: {tag} is iterable"
                    current_value.append(tag)
        post_by_id.save()
    else:
        return
def main(filepath="difference_cache.jsonl"):
    print(f"Reading differences from {filepath}")
    differences = get_differences(filepath)
    with db.atomic():# commit all changes at once
        commit_differences(differences)
    # sample random post from differences and show
    sample_post_id = choice(list(differences.keys()))
    print(f"Sample Post: {sample_post_id}")
    print(f"Sample Difference: {differences[sample_post_id]}")
    # get real state of post
    post = Post.get_by_id(sample_post_id)
    for fields in FIELDS_TO_EXTRACT:
        attr = post.__getattribute__(FIELDS_TO_EXTRACT[fields])
        if isinstance(attr, list):
            print(f"{fields}: {[tag.name for tag in attr]}")
        else:
            print(f"{fields}: {attr}")
    

FIELDS_TO_EXTRACT = {
    'id': 'id',
    'created_at': 'created_at',
    'source': 'source',
    'rating': 'rating',
    'score': 'score',
    'fav_count': 'fav_count',
    'tag_list_general': 'tag_list_general',
    'tag_list_artist': 'tag_list_artist',
    'tag_list_character': 'tag_list_character',
    'tag_list_copyright': 'tag_list_copyright',
    'tag_list_meta': 'tag_list_meta',
}

def save_post(idx:int):
    folder = "danbooru2023_fixed"
    dump = {}
    if not os.path.exists(f"{folder}/posts"):
        os.makedirs(f"{folder}/posts")
    post = Post.get_by_id(idx)
    with open(f"{folder}/posts/{idx}.json",'w') as file:
        for field in FIELDS_TO_EXTRACT:
            dump[field] = post.__getattribute__(FIELDS_TO_EXTRACT[field])
            # if list, convert to list of string instead of tag objects
            if isinstance(dump[field], list):
                dump[field] = [tag.name for tag in dump[field]]
        json.dump(dump, file)

if __name__ == "__main__":
    jsonl_files = glob.glob(r"<path to difference_cache.jsonl>")
    # sort by number
    jsonl_files.sort()
    for file in tqdm(jsonl_files):
        main(file)
    