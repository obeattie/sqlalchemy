<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="datamapping", description="Basic Data Mapping" &>

<&|doclib.myt:item, name="synopsis", description="Synopsis" &>

        <&|formatting.myt:code&>
        from sqlalchemy.schema import *
        from sqlalchemy.mapper import *
        import sqlalchemy.sqlite as sqlite
        engine = sqllite.engine(':memory:', {})
        
        # table <& formatting.myt:link, path="metadata", text="metadata" &>
        users = Table('users', engine, 
            Column('user_id', INTEGER, primary_key = True),
            Column('user_name', VARCHAR(16), nullable = False),
            Column('email_address', VARCHAR(60), key='email'),
            Column('password', VARCHAR(20), nullable = False)
        )
        
        # class definition
        class User(object):
            def __init__(self):
                pass
    
        # obtain a Mapper
        m = mapper(User, users)
        
        # select
        user = m.select(users.c.user_name == 'fred')[0]
        
        # modify
        user.user_name == 'fred jones'
        
        # commit
        objectstore.commit()

    </&>
</&>

<&|doclib.myt:item, name="onetomany", description="One to Many" &>

        <&|formatting.myt:code&>
        # second table <& formatting.myt:link, path="metadata", text="metadata" &>
        addresses = Table('email_addresses', engine,
            Column('address_id', INT, primary_key = True),
            Column('user_id', INT, foreign_key = ForeignKey(users.c.user_id)),
            Column('email_address', VARCHAR(20)),
        )
        
        # second class definition
        class Address(object):
            def __init__(self, email_address = None):
                self.email_address = email_address
    
        # obtain a Mapper.  "private=True" means deletions of the user
        # will cascade down to the child Address objects
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy=True, private=True)
        ))
        
        # select
        user = m.select(users.c.user_name == 'fred')[0]
        address = user.addresses[0]
        
        # modify
        user.user_name == 'fred jones'
        user.addresses[0].email_address = 'fredjones@foo.com'
        user.addresses.append(Address('freddy@hi.org'))
        
        # commit
        objectstore.commit()

    </&>
</&>

<&|doclib.myt:item, name="onetoone", description="One to One" &>

        <&|formatting.myt:code&>
        # a table to store a user's preferences for a site
        prefs = Table('user_prefs', engine,
            Column('pref_id', INT, primary_key = True),
            Column('stylename', VARCHAR(20)),
            Column('save_password', BOOLEAN, nullable = False),
            Column('timezone', CHAR(3), nullable = False)
        )

        # user table gets 'preference_id' column added
        users = Table('users', engine, 
            Column('user_id', INTEGER, primary_key = True),
            Column('user_name', VARCHAR(16), nullable = False),
            Column('email_address', VARCHAR(60), key='email'),
            Column('password', VARCHAR(20), nullable = False)
            Column('preference_id', INTEGER, foreign_key = ForeignKey(prefs.c.pref_id))
        )
        
        # class definition for preferences
        class UserPrefs(object):
            pass
    
        # obtain a Mapper.
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, lazy=True, private=True),
            preferences = relation(UserPrefs, prefs, lazy=False, private=True),
        ))
        
        # select
        user = m.select(users.c.user_name == 'fred')[0]
        save_password = user.preferences.save_password
        
        # modify
        user.preferences.stylename = 'bluesteel'
        
        # commit
        objectstore.commit()

    </&>
</&>

<&|doclib.myt:item, name="manytomany", description="Many to Many" &>
        <&|formatting.myt:code&>
            # create articles table.  note the usage of "key=<synonym>" to redefine the
            # names of properties that will be used on the object.
            articles = Table('articles', engine,
                Column('article_id', INT, primary_key = True),
                Column('article_headline', key='headline', VARCHAR(150)),
                Column('article_body', key='body', CLOB),
            )
            
            keywords = Table('keywords', engine,
                Column('keyword_id', INT, primary_key = True),
                Column('name', VARCHAR(50))
            )

            itemkeywords = Table('article_keywords', engine,
                Column('article_id', INT, foreign_key = ForeignKey(articles.c.article_id)),
                Column('keyword_id', INT, foreign_key = ForeignKey(keywords.c.keyword_id))
            )
            
            # class definitions
            class Article:pass
            
            class Keyword:
                def __init__(self, name = None):
                    self.name = name
                    
            # create mapper.  we will eager load keywords.  
            m = mapper(Article, articles, properties = dict(
                keywords = relation(Keyword, keywords, itemkeywords, lazy=False)
            ))
            
            # select articles based on some keywords.  the extra selection criterion 
            # won't get in the way of the separate eager load of all the article's keywords
            articles = m.select(sql.and_(keywords.c.keyword_id==articles.c.article_id, keywords.c.keyword_name.in_('politics', 'entertainment')))
            
            # modify
            del articles.keywords[articles.keywords.index('politics')]
            articles.keywords.append(Keyword('topstories'))
            articles.keywords.append(Keyword('government'))
            
            # commit.  individual INSERT/DELETE operations will take place only for the list
            # elements that changed.
            objectstore.commit()
        </&>
        
        <p>Many to Many can also be done with an Association object, that adds additional information about how two items are related:</p>
        <&|formatting.myt:code&>
            # add "attached_by" column which will reference the user who attached this keyword
            itemkeywords = Table('article_keywords', engine,
                Column('article_id', INT, foreign_key = ForeignKey(articles.c.article_id)),
                Column('keyword_id', INT, foreign_key = ForeignKey(keywords.c.keyword_id)),
                Column('attached_by', INT, foreign_key = ForeignKey(users.c.user_id))
            )

            # define an association class
            class KeywordAssociation:pass
            
            # define the mapper. when we load an article, we always want to get the keywords via
            # eager loading.  but the user who added each keyword, we usually dont need so specify 
            # lazy loading for that.
            m = mapper(Article, articles, properties=dict(
                keywords = relation(KeywordAssociation, itemkeywords, lazy = False, properties=dict(
                    keyword = relation(Keyword, keywords, lazy = False),
                    user = relation(User, users, lazy = True)
                    )
                ))
            )
            # bonus step - well, we do want to load the users in one shot, so modify the mapper via an option.
            # this returns a new mapper with the option switched on.
            m2 = mapper.options(eagerload('user'))
            
            # select by keyword again
            articles = m.select(sql.and_(keywords.c.keyword_id==articles.c.article_id, keywords.c.keyword_name == 'jacks_stories'))
            
            # user is available
            for a in articles:
                for k in a.keywords:
                    if k.keyword.name == 'jacks_stories':
                        print k.user.user_name
            
        </&>
        
</&>

</&>