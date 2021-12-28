#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim: set tabstop=4 softtabstop=4 expandtab shiftwidth=4

"""
Copyright 2015 Brno University of Technology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# Author: Tomas Ondrus, xondru04@stud.fit.vutbr.cz
# Author: Jan Cerny, xcerny62@stud.fit.vutbr.cz
# Author: Lubomir Otrusina, iotrusina@fit.vutbr.cz
# Author: Jan Doležal, xdolez52@stud.fit.vutbr.cz
#
# Description: Merges two Knowledge Bases together.

import argparse
import sys
import time
import copy
import collections
import itertools

from KbGenerateId import generateId

class KB(object):
    '''
    Class representing Knowledge Base.
    '''
    
    DEFAULT_ID_FIELDS = ["ID", "WIKIDATA URL", "WIKIPEDIA URL", "FREEBASE URL", "DBPEDIA URL", "ULAN ID", "GEONAMES ID"]
    
    def __init__(self, KB_file_name, KB_fields_file_name, separator):
        '''
        KB_file_name - file name of a KB
        KB_fields_file_name - file name of config for a KB
        separator - multiple values separator used in a KB
        '''

        self.name = KB_file_name
        if KB_fields_file_name == None: 
            self.fields_file_name = KB_file_name + ".fields"
        else:
            self.fields_file_name = KB_fields_file_name
        self.separator = separator
    
    def __contains__(self, field_name):
        return field_name in self.fields
    
    def load_config(self):
        '''
        Loads a config file with fields description (*.fields) for a given KB.
        '''

        try:
            fields_fd = open(self.fields_file_name, 'r')
        except IOError:
            printErr("Cannot open file " + self.fields_file_name + ".")
            sys.exit(1)
        self.fields = dict()
        line_number = 0
        POSTFIX_LEN = len(" (MULTIPLE VALUES)")
        for line in fields_fd: 
            line = line.strip()
            if (not line): # skips empty lines
                continue
            if (line.endswith(" (MULTIPLE VALUES)")):
                self.fields[self.name + "." + line[:-POSTFIX_LEN]] = Field(line_number, True)
            else:
                self.fields[self.name + "." + line] = Field(line_number, False)
            line_number += 1
        self.field_count = len(self.fields)
        fields_fd.close()

    def load_to_memory(self):
        '''
        Loads KB into memory.
        '''
        
        # fix Freebase URL
        freebase_name = self.name + "." + "FREEBASE URL"
        if freebase_name in self:
            freebase_idx = self.get_field_order_num(freebase_name)
        else:
            freebase_idx = None

        try:
            kb_fd = open(self.name, "r")
        except IOError:
            printErr("Cannot open file " + self.name + ".")
            sys.exit(1)
        self.entities = list()
        for line in kb_fd:
            entity = Entity(line.rstrip("\r\n"), self.separator, self.field_count)
            if freebase_idx is not None:
                entity.fixFreebaseUrl(freebase_idx)
            self.entities.append(entity)
        kb_fd.close()

    def get_field_order_num(self, field_name):
        '''
        Returns a position for a given field named field_name.
        '''

        return self.fields[field_name].order_num

class Field(object):
    '''
    Class for a value from *.fields config file.
    '''

    def __init__(self, order_num, multiple):
        self.order_num = order_num
        self.multiple  = multiple

class Entity(object):
    """
    Data line container.
    """
    
    def __init__(self, line, separator, field_count):
        self.data    = line.split("\t")
        self.weight  = 0
        self.used  = False
        self.matched = None
        
        if len(self.data) != field_count:
            raise RuntimeError("len(self.data) != field_count \nself.data: %s" % (self.data))
        
        for i in range(field_count):
            self.data[i] = self.data[i].split(separator)
            for j in range(len(self.data[i])):
                self.data[i][j] = self.data[i][j].strip()
            self.data[i] = set(self.data[i])
            if '' in self.data[i]:
                self.data[i].remove('')
            self.data[i] = list(self.data[i])
        self.data = list(self.data)
    
    def __str__(self):
        return "%(self)r:\ndata==%(data)r\nmatched==%(matched)r\nused==%(used)r\nweight==%(weight)r\n" % {"self": self, "data": self.data, "matched": self.matched, "used": self.used, "weight": self.weight}
    
    def fixFreebaseUrl(self, freebase_idx):
        new_freebase_url_list = []
        for freebase_url in self.data[freebase_idx]:
            if "freebase.com/" in freebase_url and "http://www.freebase.com/" not in freebase_url:
                fix_idx = freebase_url.index("freebase.com/")
                new_freebase_url_list.append("http://www." + freebase_url[fix_idx:])
            else:
                new_freebase_url_list.append(freebase_url)
        self.data[freebase_idx] = new_freebase_url_list
    
    def get_field(self, order_num):
        return self.data[order_num]

class Relation(object):
    '''
    Class for a relation between 1st and 2nd KB.
    '''
    
    UNIQUE = 1
    NAME = 2
    OTHER = 3
    
    def __init__(self, n1, n2, rel_type, blacklist=None):
        if blacklist is None:
            blacklist = set()
        
        self.kb1_field = n1
        self.kb2_field = n2
        self.type = rel_type
        self.blacklist = blacklist

def parse_relations(rel_conf_file_name, kb1, kb2):
    '''
    Loads relations between the KBs from config a file.

    rel_conf_file_name - name of the file with relation configuration
    kb1 - 1st KB
    kb2 - 2nd KB
    '''

    try:
        rel_fd = open(rel_conf_file_name, 'r')
    except IOError:
        printErr("Cannot open file " + rel_conf_file_name + ".")
        sys.exit(1)
    relations = list()
    rel_type = 0
    for line in rel_fd:
        if not line:
            pass
        elif line.startswith("UNIQUE:"):
            rel_type = Relation.UNIQUE
        elif line.startswith("NAME:"):
            rel_type = Relation.NAME
        elif line.startswith("OTHER:"):
            rel_type = Relation.OTHER
        elif line.startswith("\t"):
            line = line.strip()
            first,second = line.split('=')
            if first.startswith(kb2.name):
                (first, second) = (second, first)
            fieldnum1 = kb1.get_field_order_num(first)
            fieldnum2 = kb2.get_field_order_num(second)
            relations.append(Relation(fieldnum1, fieldnum2, rel_type))
        else:
            printErr("Invalid format of the config file " + rel_conf_file_name + ".")
            exit(1)
    rel_fd.close()
    return relations

def printErr(*args, **kwargs):
    if "file" not in kwargs:
        kwargs["file"] = sys.stderr
    return print(*args, **kwargs)

def make_index_for_kb1(kb1_entities, kb1_field_count, relations):
    '''
    Creates an index over the 1st KB.
    '''
    return _make_index(kb1_entities, kb1_field_count, relations, "kb1_field")

def make_index_for_kb2(kb2_entities, kb2_field_count, relations, blacklist_of_uniques=None):
    '''
    Creates an index over the 2nd KB.
    The index is used to speed up searching over the KB.
    '''
    return _make_index(kb2_entities, kb2_field_count, relations, "kb2_field")

def _make_index(kb_entities, kb_field_count, relations, kb_field):
    # determines which fields will be indexed
    blackdict = dict()
    fields_to_index = set()
    for r in relations:
        if r.type != Relation.OTHER:
            field_idx = getattr(r, kb_field)
            blackdict.setdefault(field_idx, set()).update(r.blacklist)
            fields_to_index.add(field_idx)
    
    index = [x for x in range(kb_field_count + 1)]
    
    # fulfills dicts with default empty dict
    for field in fields_to_index:
        index[field] = dict()
    
    # iterates over KB lines and add terms to index
    for entity in kb_entities:
        for field in fields_to_index:
            value = entity.get_field(field)
            if (not value): # skips empty value
                continue
            for one_value in value:
                # odfiltrování falešných unikátů
                if one_value in blackdict[field]:
                    continue
                
                # saves reference
                index[field].setdefault(one_value, set()).add(entity)
    return index

def get_args():
    """
    Parses arguments of the program. Returns an object of class argparse.Namespace.
    """ 

    parser = argparse.ArgumentParser(
        description="Compare two different Knowledge Bases.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--first",
        help="filename of the first KB (also used as a prefix for config files)",
        required=True)
    parser.add_argument("--second",
        help="filename of the second KB (also used as a prefix for config files)",
        required=True)
    parser.add_argument("--first_fields",
        help="filename of the first KB fields list (default '(--first option).fields')")
    parser.add_argument("--second_fields",
        help="filename of the second KB fields list (default '(--second option).fields')")
    parser.add_argument("--rel_conf",
        help="filename of a relationships config",
        required=True)
    parser.add_argument("--output_conf",
        help="filename of an output format",
        required=True)
    parser.add_argument("--other_output_conf",
        help="filename of an output format",
        required=True)
    parser.add_argument("--first_sep",
        help="first multiple value separator",
        default="|")
    parser.add_argument("--second_sep",
        help="second multiple value separator",
        default="|")
    parser.add_argument("--id_prefix",
        help="prefix for ids")
    parser.add_argument("--deduplicate_kb1",
        help="deduplicate_kb1",
        action='store_true')
    parser.add_argument("--deduplicate_kb2",
        help="deduplicate_kb2",
        action='store_true')
    parser.add_argument("--id_fields",
        nargs="+",
        help="names of fields with unique id for deduplication",
        default=KB.DEFAULT_ID_FIELDS)
    parser.add_argument("--output",
        help="filename of an output",
        required=True)
    parser.add_argument("--second_output",
        help="filename of output of rest not matched from second KB")
    parser.add_argument("--treshold",
        help="matching treshold",
        required=True)
    return parser.parse_args()

def uniqifyList(seq, order_preserving=True):
    if order_preserving:
        seen = set()
        return [x for x in seq if x not in seen and not seen.add(x)]
    else:
        return list(set(seq))

def countNonEmptyFields(entity):
    count = 0
    for field in entity.data:
        if field:
            count += 1
    return count

def deduplicate(kb, id_fields, blacklist_of_uniques=None):
    '''
    Deduplicate \a kb according to \a id_fields.
    '''
    relations = _getIdRelations(kb, id_fields)
    
    begin = time.time()
    index_for_kb = make_index_for_kb1(kb.entities, kb.field_count, relations)
    sys.stdout.write("Deduplication: The index for KB "+kb.name+" was created (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    return _deduplicate(kb, index_for_kb, relations, "kb1_field", blacklist_of_uniques)

def _getIdRelations(kb, id_fields):
    relations = list()
    rel_type = Relation.UNIQUE
    for field_name in id_fields:
        kb_field_name = "%s.%s" % (kb.name, field_name)
        if kb_field_name in kb:
            fieldnum1 = kb.get_field_order_num(kb_field_name)
            relations.append(Relation(fieldnum1, 0, rel_type))
    return relations

def _getIds(id_fields, entity, blacklist=None):
    result = []
    for field_idx in id_fields:
        unique_ids = entity.get_field(field_idx)
        result.extend((field_idx, id) for id in unique_ids if blacklist is None or (field_idx, id) not in blacklist)
    return result

def _collectIds(dest_unique_ids_dict, entity, index_for_kb):
    # Touto funkcí můžeme získat více různých odkazů na wikipedii, jsou-li v KB špatné informace. Pokud se tomu chceme vyhnout je lepší použít funkci _collectUniqueIds().
    for (field_idx, id) in _getIds(dest_unique_ids_dict.keys(), entity):
        if id not in dest_unique_ids_dict[field_idx]:
            dest_unique_ids_dict[field_idx].add(id)
            for match in index_for_kb[field_idx].get(id):
                _collectIds(dest_unique_ids_dict, match, index_for_kb)

def _collectUniqueIds(dest_unique_ids_dict, root_entity, index_for_kb, blacklist=None):
    if blacklist is None:
        blacklist = set()
    fifo_entity = collections.deque()
    fifo_entity.append((root_entity, ""))
    while fifo_entity:
        entity, from_id = fifo_entity.popleft()
        if from_id in blacklist:
            continue
        cur_ids = _getIds(dest_unique_ids_dict.keys(), entity, blacklist)
        candidates_ids = []
        candidates_entities = []
        for (field_idx, id) in cur_ids:
            if (field_idx, id) in blacklist: # blacklist může být změněn, proto je třeba jej stále kontrolovat
                continue
            if id not in dest_unique_ids_dict[field_idx]:
                if len(dest_unique_ids_dict[field_idx]):
                    # odstranění konfliktu a přidání konfliktních ID do blacklistu
                    new_black_ids = [(_field_idx, _id) for (_field_idx, _id) in cur_ids if _id in dest_unique_ids_dict[_field_idx] and not(dest_unique_ids_dict[_field_idx].remove(_id))]
                    blacklist.update(new_black_ids)
                    candidates_ids.clear()
                    candidates_entities.clear()
                else:
                    candidates_ids.append((field_idx, id))
                    candidates_entities.extend(zip(index_for_kb[field_idx].get(id), itertools.repeat((field_idx, id))))
        for (field_idx, id) in candidates_ids:
            dest_unique_ids_dict[field_idx].add(id)
        fifo_entity.extend(candidates_entities)

def _deduplicate(kb, index_for_kb, relations, kb_field, blacklist=None):
    if blacklist is None:
        blacklist = set()
    
    unique_ids_fields_idx = set()
    for r in relations:
        if r.type == Relation.UNIQUE:
            unique_ids_fields_idx.add(getattr(r, kb_field))
    
    # going through entities from the 1st KB
    new_kb_entities = []
    for entity in kb.entities:
        if entity.used == True:
            continue
        
        unique_ids_dict = {}
        for field_idx in unique_ids_fields_idx:
            unique_ids_dict[field_idx] = set()
        _collectUniqueIds(unique_ids_dict, entity, index_for_kb, blacklist)
        
        matches = set()
        for field_idx in unique_ids_dict:
            for id in unique_ids_dict[field_idx]:
                matches.update(index_for_kb[field_idx].get(id))
        matches = list(matches)
        
        if len(matches) > 1:
            # seřadit dle počtu neprázdných sloupců
            matches.sort(key=countNonEmptyFields, reverse=True)
            new_entity = copy.copy(matches[0])
            for match in matches:
                match.used = True # marked as used (can be used only once)
                for field_idx in range(len(entity.data)):
                    new_entity.data[field_idx].extend(match.data[field_idx])
            new_entity.data = [uniqifyList(field, order_preserving=True) for field in new_entity.data]
            
            for field_head in kb.fields.values():
                if not field_head.multiple: # Více hodnot může mít pouze ten sloupec, který má příznak (MULTIPLE VALUES).
                    field = new_entity.data[field_head.order_num]
                    if len(field) > 1:
                        field = field[0:1]
                        new_entity.data[field_head.order_num] = field
            
            new_kb_entities.append(new_entity)
        else:
            new_kb_entities.append(entity)
    
    print("Deduplication: Number of removed entities ==", len(kb.entities) - len(new_kb_entities))
    print("Deduplication: Created this blacklist of URIs as corrupted keys attributes ( length ==", len(blacklist), "):", sorted(blacklist))
    kb.entities = new_kb_entities

def _checkUnique(entity_from_kb1, entity_from_kb2, index_for_kb1, unique_relations):
    '''
    Zkontroluje, zda po sjednocení \a entity_from_kb1 a \a entity_from_kb2 nedojde ke konfliktu identifikátorů. Následně je třeba index aktualizovat.
    '''
    
    is_unique = True
    for relation in unique_relations:
        id_list = entity_from_kb2.get_field(relation.kb2_field)
        for value in id_list:
            if value:
                matched_entities = index_for_kb1[relation.kb1_field].get(value, tuple())
                if len(matched_entities) > 1 or len(matched_entities) == 1 and next(iter(matched_entities)) is not entity_from_kb1:
                    is_unique = False
                    break
    return is_unique

def _getCheckUniqueErrorUriList(entity_from_kb1, entity_from_kb2, index_for_kb1, unique_relations):
    kb1 = set()
    kb2 = set()
    is_unique = True
    for relation in unique_relations:
        kb1.update(entity_from_kb1.get_field(relation.kb1_field))
        id_list = entity_from_kb2.get_field(relation.kb2_field)
        kb2.update(id_list)
        for value in id_list:
            if value:
                matched_entities = index_for_kb1[relation.kb1_field].get(value, tuple())
                if len(matched_entities) > 1 or len(matched_entities) == 1 and next(iter(matched_entities)) is not entity_from_kb1:
                    is_unique = False
                    for e in matched_entities:
                        for r in unique_relations:
                            kb1.update(e.get_field(r.kb1_field))
    
    if is_unique:
        return set(), set()
    else:
        return kb1, kb2

def _updateUniqueInIndex(entity_from_kb1, index_for_kb1, unique_relations):
    '''
    Aktualizuje \a unique_relations v \a index_for_kb1 pomocí \a entity_from_kb1
    '''
    
    if entity_from_kb1.matched is not None:
        for relation in unique_relations:
            id_list = entity_from_kb1.matched.get_field(relation.kb2_field)
            for value in id_list:
                if value:
                    if value in relation.blacklist:
                        continue
                    else:
                        index_for_kb1[relation.kb1_field].setdefault(value, set()).add(entity_from_kb1)
    return index_for_kb1

def match(kb1, index_for_kb1, index_for_kb2, relations, treshold):
    '''
    Matches items from the 2nd KB with the corresponding items from the 1st KB.
    '''

    # dividing relations into three list according the type
    unique_relations = list()
    name_relations = list()
    other_relations = list()
    for x in relations:
        if x.type == Relation.UNIQUE:
            unique_relations.append(x)
        elif x.type == Relation.NAME:
            name_relations.append(x)
        else:
            other_relations.append(x)

    # going through entities from the 1st KB
    for entity in kb1.entities:

        # first, searching for a corresponding entry from the 2nd KB based on unique ids
        match = match_by_unique(entity, index_for_kb2, unique_relations)
        if match is not None:
            # Předcházení konfliktu identifikátorů, tedy aby po sloučení dvou entit nevznikly nové konflikty
            if _checkUnique(entity, match, index_for_kb1, unique_relations):
                entity.matched = match
                match.used = True # marked as used (can be used only once)
                entity.used = True
                _updateUniqueInIndex(entity, index_for_kb1, unique_relations) # update index for kb1
            else:
                # Při spojování entit pomocí identifikátorů by nemělo dojít ke konfliktu identifikátorů, ale pokud tato situace nastane, bude dobré vypsat URI entit, které byly v konfliktu
                kb1_error_uri_list, kb2_error_uri_list = _getCheckUniqueErrorUriList(entity, match, index_for_kb1, unique_relations)
                printErr("=== match_by_unique error ===")
                printErr("<<<<<<<<<")
                printErr(kb1_error_uri_list)
                printErr("---------")
                printErr(kb2_error_uri_list)
                printErr(">>>>>>>>>")
            continue  # process another entry from KB

        # second, searching for a corresponding entry from the 2nd KB based on names (aliases, ...)
        candidates = match_by_name(entity, index_for_kb2, name_relations)
        if not candidates:
            continue

        # getting score for each candidate
        for candidate in candidates:
            for relation in unique_relations:
                first = entity.get_field(relation.kb1_field)
                second = candidate.get_field(relation.kb2_field)
                if first and second and first[0] != second[0]:
                    candidate.weight = -1000
                    break
            
            # Předcházení konfliktu identifikátorů, tedy aby po sloučení dvou entit nevznikly nové konflikty
            if not _checkUnique(entity, candidate, index_for_kb1, unique_relations):
                candidate.weight = -999
            
            # evaluating in name_relations was performed during the match_by_name function call
            if (candidate.weight < treshold): 
                continue
            for relation in other_relations:
                first = entity.get_field(relation.kb1_field)
                second = candidate.get_field(relation.kb2_field)
                for i in first:
                    for j in second:
                        try: # if string contains a number, the number will be rounded 
                            i = round(float(i), 1)
                        except:
                            pass
                        try:
                            j = round(float(j), 1)
                        except:
                            pass
                        if i == j:
                            candidate.weight += 1

        # choosing the best candidate (the one with the highest score)
        if candidates:
            best = next(iter(candidates))
            for candidate in candidates:
                if candidate.weight > best.weight:
                    best = candidate

            # links the best one with the particular entity
            if best.weight >= treshold:
                entity.matched = best
                entity.used = True
                best.used = True # mark as used (can be used only once)

        # set the score to zero
        for candidate in candidates:
            candidate.weight = 0
        
        # update index for kb1
        _updateUniqueInIndex(entity, index_for_kb1, unique_relations)

def match_by_unique(entity, index, unique_relations):
    '''
    Searches over the index for the corresponding entities based on the unique id.
    '''

    for relation in unique_relations:
        id_list = entity.get_field(relation.kb1_field)
        for value in id_list:
            if value:
                match = index[relation.kb2_field].get(value, tuple())
                for e in match:
                    if not e.used:
                        return e
    return None               

def match_by_name(entity, index, name_relations):
    '''
    Searches over the index for the corresponding entities based on the name comparison.
    '''

    candidates = set()
    for relation in name_relations:
        name_list = entity.get_field(relation.kb1_field)
        for value in name_list: # if empty, we have to take another relation
            if value:
                match = index[relation.kb2_field].get(value, tuple())
                for e in match:
                    if not e.used:
                       e.weight += 1
                       candidates.add(e)
    return candidates
 
class Output(object):
    '''
    Class for creating the output.
    '''

    def __init__(self, output_conf_file_name, other_output_conf_file_name, output_file_name, second_output_file_name=None):
        try:
            output_conf_fd = open(output_conf_file_name, 'r')
        except IOError:
            printErr("Cannot open file " + output_conf_file_name + ".")
            sys.exit(1)
        output_fields = list()
        for line in output_conf_fd:
            line = line.strip() # trim whitespaces
            if (not line): # skip empty lines
                continue
            if (line == "None"):
                output_fields += [None]
            else:
                output_fields += [line]
        output_conf_fd.close()

        try:
            other_output_conf_fd = open(other_output_conf_file_name, 'r')
        except IOError:
            printErr("Cannot open file " + other_output_conf_file_name + ".")
            sys.exit(1)
        other_output_fields = list()
        for line in other_output_conf_fd:
            line = line.strip() # trim whitespaces
            if (not line): # skip empty lines
                continue
            if (line == "None"):
                other_output_fields += [None]
            else:
                other_output_fields += [line]
        other_output_conf_fd.close()

        try:
            self.output_fd = open(output_file_name, 'w')
        except IOError:
            printErr("Cannot open file " + output_file_name + ".")
            sys.exit(1)

        if second_output_file_name is not None:
            try:
                self.second_output_fd = open(second_output_file_name, 'w')
            except IOError:
                printErr("Cannot open file " + second_output_file_name + ".")
                sys.exit(1)
            self.second_output = True
        else:
            self.second_output = False

        self.output_fields = output_fields
        self.other_output_fields = other_output_fields

    def _generateId(self):
        self.counter += 1
        result = generateId(self.prefix, self.counter)
        return result

    def make_output(self, kb1, kb2, relations, prefix):
        '''
        Creates the output.
        '''

        self.counter = 0 # global ID counter
        self.prefix = prefix
        kb1_matched = 0
        kb1_not_matched = 0
        for line in kb1.entities:
            generated_line = list()
            if line.used:
                kb1_matched += 1
                for fieldname in self.output_fields:
                    if (fieldname == "ID"):
                        generated_line.append([self._generateId()])
                    elif (fieldname == None):
                        generated_line.append([""])
                    elif (fieldname.startswith('"')):
                        generated_line.append([fieldname.strip('"')])
                    else:
                        possible = list()
                        if fieldname.startswith(kb1.name):
                            ff = kb1.fields[fieldname]
                            possible.extend(line.get_field(ff.order_num))
                            if ff.multiple or len(possible) == 0 : # using values from the 2nd KB
                                for relation in relations:
                                    if relation.kb1_field == ff.order_num:
                                        possible.extend(line.matched.get_field(relation.kb2_field)) 
                        else:
                            ff = kb2.fields[fieldname]
                            if not line.matched:
                                print(line)
                            possible.extend(line.matched.get_field(ff.order_num))
                            if ff.multiple or len(possible) == 0 : # using values from the 1st KB
                                for relation in relations:
                                    if relation.kb2_field == ff.order_num:
                                        possible.extend(line.get_field(relation.kb1_field)) 
                        possible = uniqifyList(possible, order_preserving=False)
                        if not ff.multiple and len(possible) > 1:
                            possible = possible[0:1]
                        generated_line.append(possible)
            else:
                kb1_not_matched += 1
                for fieldname in self.other_output_fields:
                    if (fieldname == "ID"):
                        generated_line.append([self._generateId()])
                    elif (fieldname == None):
                        generated_line.append([""])
                    elif (fieldname.startswith('"')):
                        generated_line.append([fieldname.strip('"')])
                    else:
                        possible = list()
                        for fn in fieldname.split("|"):
                            field = kb1.get_field_order_num(fn)
                            possible.extend(line.get_field(field))
                        possible = uniqifyList(possible, order_preserving=False)
                        generated_line.append(possible)
            self.write_line_to_output(generated_line)
        sys.stdout.write("Matched entities : " + str(kb1_matched) + ".\n")
        sys.stdout.write("Unmatched entities from the 1st KB : " + str(kb1_not_matched) + ".\n")
        if self.second_output:
            self.generate_rest(kb2.entities)
        else:
            self.generate_second(kb2.entities, self.output_fields, kb2.name, kb2.fields)
        self.output_fd.close()

    def write_line_to_output(self, line):
        result = "\t".join("|".join(field) for field in line) + "\n"
        self.output_fd.write(result)

    def generate_second(self, data, output, kb, fields_in_kb2):
        kb2_not_matched = 0
        for line in data:
            if line.used:
                continue
            kb2_not_matched += 1
            result = []
            for fieldname in output:
                if (fieldname == "ID"):
                    result.append(self._generateId())
                elif (fieldname == None):
                    result.append("")
                elif (fieldname.startswith('"')):
                    result.append(fieldname.strip('"'))
                elif (not fieldname.startswith(kb)):
                    result.append("")
                else:
                    result.append("|".join(line.get_field(fields_in_kb2[fieldname].order_num)))
         
            result = "\t".join(result) + "\n"
            self.output_fd.write(result)
        sys.stdout.write("Unmatched entities from the 2nd KB : " + str(kb2_not_matched) + ".\n")

    def generate_rest(self, entities):
        sys.stdout.write("Unmatched entities from the 2nd KB were written into the separate file.\n")
        kb2_not_matched = 0
        for e in entities:
            if e.used:
                continue
            kb2_not_matched += 1
            line = "\t".join("|".join(field) for field in e.data) + "\n"
            self.second_output_fd.write(line)
        sys.stdout.write("Unmatched from the 2nd KB " + str(kb2_not_matched) + ".\n")

def main():
    args = get_args()
    output_maker = Output(args.output_conf, args.other_output_conf, args.output, args.second_output)
    
    begin = time.time()
    kb1 = KB(args.first, args.first_fields, args.first_sep)
    kb1.load_config()
    kb1.load_to_memory()
    sys.stdout.write("The 1st KB, " + kb1.name + ", was loaded into memory (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    begin = time.time()
    kb2 = KB(args.second, args.second_fields, args.second_sep)
    kb2.load_config()
    kb2.load_to_memory()
    sys.stdout.write("The 2nd KB, " + kb2.name + ", was loaded into memory (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    relations = parse_relations(args.rel_conf, kb1, kb2)
    
    if args.deduplicate_kb1:
        blacklist_of_uniques = set()
        
        begin = time.time()
        deduplicate(kb1, args.id_fields, blacklist_of_uniques)
        sys.stdout.write("The 1st KB, " + kb1.name + ", was successfully deduplicated (" + str(round(time.time() - begin, 2)) + " s).\n")
        
        blackdict_of_uniques = dict()
        for field_idx, value in blacklist_of_uniques:
            blackdict_of_uniques.setdefault(field_idx, set()).add(value)
        for r in relations:
            if r.type == Relation.UNIQUE and r.kb1_field in blackdict_of_uniques:
                r.blacklist.update(blackdict_of_uniques[r.kb1_field])
    
    if args.deduplicate_kb2:
        blacklist_of_uniques = set()
        
        begin = time.time()
        deduplicate(kb2, args.id_fields, blacklist_of_uniques)
        sys.stdout.write("The 2nd KB, " + kb2.name + ", was successfully deduplicated (" + str(round(time.time() - begin, 2)) + " s).\n")
        
        blackdict_of_uniques = dict()
        for field_idx, value in blacklist_of_uniques:
            blackdict_of_uniques.setdefault(field_idx, set()).add(value)
        for r in relations:
            if r.type == Relation.UNIQUE and r.kb2_field in blackdict_of_uniques:
                r.blacklist.update(blackdict_of_uniques[r.kb2_field])
    
    begin = time.time()
    index_for_kb1 = make_index_for_kb1(kb1.entities, kb1.field_count, relations)
    sys.stdout.write("The index for the 1st KB was created for compare (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    begin = time.time()
    index_for_kb2 = make_index_for_kb2(kb2.entities, kb2.field_count, relations)
    sys.stdout.write("The index for the 2nd KB was created for compare (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    begin = time.time()
    match(kb1, index_for_kb1, index_for_kb2, relations, int(args.treshold))
    sys.stdout.write("KBs " + kb1.name + " and " + kb2.name + " were successfully compared (" + str(round(time.time() - begin, 2)) + " s).\n")
    
    begin = time.time()
    output_maker.make_output(kb1, kb2, relations, args.id_prefix)
    sys.stdout.write("A new KB " + args.output + " was created (" + str(round(time.time() - begin, 2)) + " s).\n")
    return 0

if __name__ == "__main__":
    sys.exit(main())

# konec souboru kb_compare.py
