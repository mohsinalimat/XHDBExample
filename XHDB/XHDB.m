//
//  XHDB.m
//  XHDBExample
//
//  Created by xiaohui on 16/6/2.
//  Copyright © 2016年 qiantou. All rights reserved.
//

#import "XHDB.h"
#import "FMDB.h"


#ifdef DEBUG
#define debugLog(...)    NSLog(__VA_ARGS__)
#else
#define debugLog(...)
#endif

#define PATH_OF_DOCUMENT    [NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES) objectAtIndex:0]

typedef void(^finishBlock)(BOOL );

@interface XHDB()

@property (strong, nonatomic) FMDatabaseQueue * dbQueue;
@property (copy, nonatomic ) finishBlock finish;

@end

@implementation XHDB

static NSString *const DEFAULT_DB_NAME = @"database.sqlite";

static NSString *const CREATE_TABLE_SQL =
@"CREATE TABLE IF NOT EXISTS %@ ( \
id TEXT NOT NULL, \
json TEXT NOT NULL, \
createdTime TEXT NOT NULL, \
PRIMARY KEY(id)) \
";

//static NSString *const UPDATE_ITEM_SQL = @"REPLACE INTO %@ (id, json, createdTime) values (?, ?, ?)";

static NSString *const QUERY_ITEM_SQL = @"SELECT json, createdTime from %@ where id = ? Limit 1";

static NSString *const SELECT_ALL_SQL = @"SELECT * from %@";

static NSString *const COUNT_ALL_SQL = @"SELECT count(*) as num from %@";

static NSString *const CLEAR_ALL_SQL = @"DELETE from %@";

static NSString *const DELETE_ITEM_SQL = @"DELETE from %@ where id = ?";

static NSString *const DELETE_ITEMS_SQL = @"DELETE from %@ where id in ( %@ )";

static NSString *const DELETE_ITEMS_WITH_PREFIX_SQL = @"DELETE from %@ where id like ? ";

/**
 *  初始化创建数据库文件
 */
-(id)initWithDBName:(NSString *)dbName dbPath:(NSString *)dbPath
{

    self = [super init];
    if(self)
    {
        NSString *name = DEFAULT_DB_NAME;
        NSString *path = PATH_OF_DOCUMENT;
        if(dbName)
        {
            name = dbName;
        }
        if(dbPath)
        {
            path = dbPath;
        }
        NSString *databasePath = [path stringByAppendingPathComponent:name];
        debugLog(@"databasePath=%@",databasePath);
        if(_dbQueue)
        {
           [_dbQueue close];
            _dbQueue = nil;
        }
        _dbQueue = [FMDatabaseQueue databaseQueueWithPath:dbPath];
        
    }
    
    return self;
}
/**
 *  检测表名
 */
+ (BOOL)checkTableName:(NSString *)tableName {
    if (tableName == nil || tableName.length == 0 || [tableName rangeOfString:@" "].location != NSNotFound) {
        debugLog(@"ERROR, table name: %@ format error.", tableName);
        return NO;
    }
    return YES;
}
/**
 *  创建表
 */
-(BOOL)createTableWithName:(NSString *)tableName
{
    if(![XHDB checkTableName:tableName]) return NO;
    
    NSString * sql = [NSString stringWithFormat:CREATE_TABLE_SQL, tableName];
    
    __block BOOL result;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        result = [db executeUpdate:sql];
    }];
    if (!result) {
        debugLog(@"ERROR, failed to create table: %@", tableName);
    }
    return result;
}
#pragma mark-save
#pragma mark-saveObject
/**
 *  存储/更新对象(异步)
 */
-(void)saveObject_async:(id)object withId:(NSString *)objectId inTable:(NSString *)tableName finish:(finishBlock)finish
{
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        
        [self saveObject:object withId:objectId inTable:tableName finish:finish];
    });
    
}
/**
 *  存储/更新对象(同步)
 */
-(BOOL)saveObject:(id)object withId:(NSString *)objectId inTable:(NSString *)tableName
{
    return [self saveObject:object withId:objectId inTable:tableName finish:nil];
}
-(BOOL)saveObject:(id)object withId:(NSString *)objectId inTable:(NSString *)tableName finish:(finishBlock)finish

{
    if(![XHDB checkTableName:tableName]) return NO;
    
    NSError * error;
    NSData * data = [NSJSONSerialization dataWithJSONObject:object options:0 error:&error];
    if (error) {
        debugLog(@"ERROR, faild to get json data");
        return NO;
    }
    NSString * jsonString = [[NSString alloc] initWithData:data encoding:(NSUTF8StringEncoding)];
    NSDate * createdTime = [NSDate date];
    NSString * sql = [NSString stringWithFormat:@"REPLACE INTO %@ (id, json, createdTime) values (?, ?, ?)", tableName];
    __block BOOL result;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        result = [db executeUpdate:sql, objectId, jsonString, createdTime];
    }];
    if (!result) {
        debugLog(@"ERROR, failed to insert/replace into table: %@", tableName);
        return  NO;
    }
    
    if(finish)
    {
        finish(result);
    }
    return YES;
}
#pragma mark-saveString
/**
 *  存储/更新字符串(异步)
 */
-(void)saveString_saync:(NSString *)string withId:(NSString *)stringId inTable:(NSString *)tableName finish:(finishBlock)finish
{
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        
        [self saveString:string withId:stringId inTable:tableName];
    });
}
/**
 *  存储/更新字符串(同步)
 */
-(BOOL)saveString:(NSString *)string withId:(NSString *)stringId inTable:(NSString *)tableName
{
    if(string==nil)
    {
        debugLog(@"ERROR, string is nil");
        return NO;
    }
    
    return [self saveObject:@[string] withId:stringId inTable:tableName finish:nil];
}

#pragma mark-get
/**
 *  获取对象
 */
-(id)getObjectById:(NSString *)objectId fromTable:(NSString *)tableName
{
     if(![XHDB checkTableName:tableName]) return nil;
    NSString * sql = [NSString stringWithFormat:QUERY_ITEM_SQL, tableName];
    __block NSString * json = nil;
    __block NSDate * createdTime = nil;
    [_dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet * rs = [db executeQuery:sql, objectId];
        if ([rs next]) {
            json = [rs stringForColumn:@"json"];
            createdTime = [rs dateForColumn:@"createdTime"];
        }
        [rs close];
    }];
    if (json) {
        NSError * error;
        id result = [NSJSONSerialization JSONObjectWithData:[json dataUsingEncoding:NSUTF8StringEncoding]
                                                    options:(NSJSONReadingAllowFragments) error:&error];
        if (error) {
            debugLog(@"ERROR, faild to prase to json");
            return nil;
        }
        return result;
    } else {
        return nil;
    }
}
/**
 *  获取字符串
 */
-(NSString *)getStringById:(NSString *)stringId fromTable:(NSString *)tableName
{
    NSString *resString = [self getObjectById:stringId fromTable:tableName];
    return resString;
}
@end
