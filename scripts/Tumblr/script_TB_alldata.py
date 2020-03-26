#!/usr/bin/env python
# -*- coding: utf8 -*-

# _____Tumblr API:   ______#

# Autor:
#      °  Palomino Alan Jesús

#_____________ LIBRARIES ______________________#
import pytumblr
import json
import pandas as pd
from sqlalchemy import create_engine
"""

The default supported types:

    state - a string, the state of the post. Supported types are published, draft, queue, private
    tags - a list, a list of strings that you want tagged on the post. eg: [“testing”, “magic”, “1”]
    tweet - a string, the string of the customized tweet you want. eg: “Man I love my mega awesome post!”
    date - a string, the customized GMT that you want
    format - a string, the format that your post is in. Support types are html or markdown
    slug - a string, the slug for the url of the post you want

"""

class Tumblr():
    def __init__(self, dbengine):
        self.dbengine = dbengine
        self.conn = self.dbengine.connect()
        # self.cur = self.conn.cursor()
        # self.blogName = 'dandelionwineshop.tumblr.com'

        # Authenticate via API Key in Tumblr

        # client = pytumblr.TumblrRestClient('WLR58xAslQgoi0Bw82NYZZ5Hnoga10Hd3yS2soeOPufQ8oO4U0')
        self.client = pytumblr.TumblrRestClient(
        'LTe2titCSZMUhdeCZRW3VAU0xnt1WsrUitH7zYLioIrej7JpUE',
        'TCGEnQDNfi00ISoFwHiDdNilX4ThBroB3Ge9YdPrZeUDjgKK3k',
        '81ITvid8cZ2ZtDFk6DDcBsZtTTm2w7VLSBGZvMrmKthvXhOdGE',
        'XIwD9OolizKxSqAebPe4dEXje3Qzti9Psp0QuskVGgfL5ow3HC')

    # Method for Aquiring Client Info
    def User_Methods(self):
        """
        returns general information about the client's blog, such as the title,
        number of posts, and other high-level data.
        """
        try:
            Uinfo = self.client.info() # get information about the authenticating user
            Udashboard = self.client.dashboard() # get the dashboard for the authenticating user
            Ulikes = self.client.likes() # get the likes for the authenticating user
            Ufollows = self.client.following() # get the blogs followed by the authenticating user
            
            #client.follow('codingjester.tumblr.com') # follow a blog
            #client.unfollow('codingjester.tumblr.com') # unfollow a blog

            #client.like(id, reblogkey) # like a post
            #client.unlike(id, reblogkey) # unlike a post
        except:
            print("Request failed!") 
        
        return  Uinfo, Udashboard, Ulikes, Ufollows

    
    # Make the request for General Blog Info
    def blog_Info(self,blogName):
        blog_info = self.client.blog_info(blogName)
        return blog_info    

    
    # Blog Methods

    def Blog_Methods(self,blogName,request):
        req = int(request)
        try:
            if req == 0:

                "Blog Info"
                b_json = self.client.blog_info(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 1:

                "Blog Avatar"
                b_json = self.client.avatar(blogName) 
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 2:

                "Blog Likes"
                b_json = self.client.blog_likes(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 3:

                "Blog Followers"
                b_json = self.client.followers(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 4:

                "Blogs Followed by the blog"
                b_json = self.client.blog_following(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 5:

                "Blog Queue"
                b_json =  self.client.queue(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            elif req == 6:

                "Blog Submissions"
                b_json = self.client.submission(blogName)
                dfItem = pd.DataFrame.from_records(b_json)
                return b_json, dfItem

            else:
                print("Invalid Request Key. Try Again.")  


            #b_info = self.client.blog_info(blogName) # get information about a blog
            #client.posts(blogName,'wine') # get posts for a blog
            #b_avatar = self.client.avatar(blogName) # get the avatar for a blog
            #b_likes = self.client.blog_likes(blogName) # get the likes on a blog
            "Cada Post contiene:"
            #b_likes['liked_posts'][0].keys()
            #b_followers = self.client.followers(blogName) # get the followers of a blog
            #b_following = self.client.blog_following(blogName) # get the publicly exposed blogs that [blogName] follows
            #b_queue = self.client.queue(blogName) # get the queue for a given blog
            #b_subs = self.client.submission(blogName) # get the submissions for a given blog
        except:
            print("Invalid Request. Try Again.") 
        
    
    # Tagged Posts
    def blog_tags(self,tag_to_search):
        tags = self.client.tagged(tag_to_search)
        #dfTags = pd.DataFrame.from_records(tags)
        df_csv = tags.to_csv('/home/ajrp/VirtEnv/src/MESURA/LF/Tags_output.csv', sep='\t', encoding='utf-8',index=False)
        
        return tags,df_csv

# Creating Engine
# dbengine=create_engine("postgres://{user}:{password}@{host}/{database}".format(user="pgcslab",password="Cslab1y.",host="cslab.cokyn0ewjjku.us-east-2.rds.amazonaws.com",database="postgres"),echo=True)
dbengine=create_engine("postgres://{user}:{password}@{host}/{database}".format(user="alanjr.palomino@gmail.com",password="alanjr",host="127.0.0.1",database="postgres"),echo=True)

ObjetoTumblr = Tumblr(dbengine)
print("|___________ Request keywords____________|") 
print("| 0   --> get information about a blog   |")
print("| 1   --> get the avatar for a blog      |")
print("| 2   --> get the likes on a blog        |")
print("| 3   --> get the followers of a blog    |")
print("| 4   --> get the blogs that self follows|")
print("| 5   --> get the queue for a given blog |")
print("| 6   --> get the submissions for a blog |")
print("|________________________________________|")
    
# Testing Tags
ObjetoTumblr.blog_tags('alitas')


