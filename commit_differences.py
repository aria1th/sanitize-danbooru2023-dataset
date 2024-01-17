"""
Reads and commits differences from difference_cache.jsonl to database
Returns number of differences committed
"""

from db import *
import os
import json
from tqdm import tqdm
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
                    tag = Tag.get_or_none(Tag.name == values)
                    if tag is None:
                        tag = Tag.create(name=values,type=keys,popularity=0)
                    current_value.append(tag)
        post_by_id.save()
    else:
        return
def main():
    differences = get_differences("difference_cache.jsonl")
    #commit_differences(differences)
    for difference_id in differences:
        if difference_id != 5522353:
            continue # get example of difference
        print(difference_id)
        print(differences[difference_id])
        keyset = set()
        keyset.update(differences[difference_id][0].keys())
        keyset.update(differences[difference_id][1].keys())
        post = Post.get_by_id(difference_id)
        for keys in keyset:
            attr = post.__getattribute__(keys)
            if isinstance(attr, list):
                print(f"{keys}: {len(attr)}")
                for values in attr:
                    print(f"\t{values.name}")
            else:
                print(f"{keys}: {attr}")
        print()
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
    main()
