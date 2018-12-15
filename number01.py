# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 16:17:44 2018

@author: chen
"""


import queue

def generate(string, Dict):
    index = 0
    q = queue.Queue()
    q.put(list(string))
    
    while index < len(string):
        if string[index] in Dict:
            for i in range(q.qsize()):
                temp = q.get()
                for e in Dict[string[index]]:
                    temp[index] = e
                    q.put(temp.copy())                    
        index += 1
        
    while q.qsize():
        print(''.join(q.get()))


if __name__ == '__main__':
    #测试用例1
    string='adcbf'
    Dict= {'a': ['B', 'C'], 'b': ['X']}
    '''
    #测试用例2
    string='adcbf'
    Dict= {'a': ['B', 'C'], 'b': ['X','Y']}
    
    #测试用例3
    string='adcbf'
    Dict= {'a': ['B', 'C'], 'b': ['X','Y','Z'],'c':['W','E']}
    
    #测试用例4
    string='aabc'
    Dict= {'a': ['B', 'C'], 'b': ['X']}
    '''
    generate(string, Dict)


            

            


