# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 18:28:47 2018

@author: chen
"""

'''
计算 TF-IDF

词频=某个词在文章中的出现次数/文章总次数
逆文本频率=log(语料库的文档总数/包含该词的文档数+1)
TF-IDF=词频（TF）*逆文本频率（IDF）

--------------------- 
def tf(word, count):
    return count[word] / sum(count.values())

def n_containing(word, count_list):
    return sum(1 for count in count_list if word in count)

def idf(word, count_list):
    return math.log(len(count_list) / (1 + n_containing(word, count_list)))

def tfidf(word, count, count_list):
    return tf(word, count) * idf(word, count_list)
--------------------- 

Scikit-Learn包中的TfidfVectorizer()函数已经实现TF-IDF
示例：
vectorizer = TfidfVectorizer(stop_words=stpwrdlst, sublinear_tf = True, max_df = 0.5)

'''


'''
假如文本数据量很大无法全量载入到单机内存完成，如何利用多台机器实现分布式的操作，请用 map-reduce 或 MPI 框架完成。

map-reduce是一种分布式计算模型。MR由两个阶段组成：map和reduce

'''

#map.py
import sys
#当前这一篇文章的总词数
everyWord = 0

for line in sys.stdin:
	ss = line.strip().split('\t')
	if len(ss) != 2:
		continue
	fileName,fileContent = ss
	#定义一个set集合，去除当前文章的重复词的个数，为了统计该词在508篇文章出现的文章次数
	word_set = set()
	#定义字典类型，为了统计当前文章每个词出现的次数，方便reduce计算tf值
	word_dict = {}	
	for content in fileContent.strip().split(' '):
		s = content.strip()
		#如果字典里面没有该单词，给字典的key=word value= 1，如果已经存在，直接修改value值进行+1
		if s not in word_dict:
			word_dict[s] = 1
		else:
			word_dict[s] += 1
		word_set.add(s)

		everyWord = len(fileContent.strip().split(' '))

	for word in word_set:
		if word in word_dict:
			print '%s\t%s\t%s\t%s\t%s' %(word,1,word_dict[word],everyWord,fileName)
   

#reduce.py
import sys
import math

doc = 508
sum = 0
cur_word = None
idf_dict = {}
tmp_dict = {}
for line in sys.stdin:
	ss = line.strip().split('\t')
	if len(ss) != 5:
		continue
	word ,cnt,everyCnt,everyWord,filename = ss
	#根据map统计的每篇文章该word出现的次数／每篇文章的总词数
	tf = float(everyCnt)/float(everyWord)
	if cur_word == None:
		cur_word = word
	if cur_word != word:
		#文章idf值，总的文章篇数／该词出现的文章篇数，这里需要注意的是，idf计算的时候是经过partition排好序后的结果
		idf =math.log(float(doc)/(float(sum)+1.0))
		#当单词不同的时候，把各自文章里面相同的单词计算tfid值，并输出
		for keyvalues in tmp_dict.items():
			#key取字典里面的key，value取对应key的tf值
			key = keyvalues[0]
			tfValue = keyvalues[1]
			#为了获取单词和文章名，把key分割开，先判断|的位置，然后再获取
			n = key.index('|')
			wd = key[:n]
			docname = key[n+2:]
			tfidf = tfValue * idf
			print '%s\t%s\t%s' %(docname,wd,tfidf)
		sum = 0		
		cur_word = None
		tmp_dict = {}
	#如果当前单词和匹配单词相同，把sum值加1，把当前单词放入字典里，为了防止key相同value被覆盖，key用单词+‘|’+文章名组成，value值为tf
	sum += int(cnt)
	key = word+'|'+filename
	tmp_dict[key] =	tf

idf =math.log(float(doc)/(float(sum)+1.0))
for keyvalues in tmp_dict.items():
	#key取字典里面的key，value取对应key的tf值
	key = keyvalues[0]
	tfValue = keyvalues[1]
	#为了获取单词和文章名，把key分割开，先判断|的位置，然后再获取
	n = key.index('|')
	wd = key[:n]
	docname = key[n+2:]
	tfidf = tfValue * idf
	print '%s\t%s\t%s' %(docname,wd,tfidf)
 
 
#conbiner.py
import sys
import os

file_input_path = sys.argv[1]
print file_input_path

def red_content(f):
	content = open(f,'r')
	return content

for fd in os.listdir(file_input_path):
	
	file_path = file_input_path + '/' +fd
		
	content_list = []
	content = red_content(file_path)
	for ss in content:
	     content_list.append(ss.strip())
	print '\t'.join([str(fd), ' '.join(content_list)])	