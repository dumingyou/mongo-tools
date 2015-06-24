// @file sync.cpp

/*
 *    Copyright (C) 2013-2015 by ccj@duoyun.org.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/pch.h"
#include "mongo/base/initializer.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/util/password.h"
#include "mongo/util/log.h"
#include "mongo/tools/tool.h" 
#include "mongo/tools/sync.h"
#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

using namespace mongo;
namespace po = boost::program_options;  

string OpTimeTool::fmtOpTime(const OpTime &t) {
    stringstream ss;
    ss << t.getSecs() << "," << t.getInc() << " (" << t.toStringLong() << ")";
    return ss.str();
}  

void OpTimeTool::setOptime( const string &optime, boost::scoped_ptr<OpTime>& _ptr ) {
      string optimets,inc;         
      if(!optime.empty()) {
       size_t i = optime.find_first_of(',');
       if ( i != string::npos ) {
           if ( i + 1 < optime.length() ) {
               inc = optime.substr(i + 1);
           }

           optimets = optime.substr(0, i);
       }

       try {
           _ptr.reset(new OpTime(
               boost::lexical_cast<unsigned long>(optimets.c_str()),
               boost::lexical_cast<unsigned long>(inc.c_str())));
       } catch( const boost::bad_lexical_cast& error) {
           log() << "Could not parse optime  into Timestamp from values ( "
                 << optimets  << " , " << inc << " )"
                 << endl;
       }
     }
}

void OplogPlayer::applyOps(boost::shared_ptr<DBClientConnection> _t, const BSONObj& op) {
  BSONObj o = op.getObjectField("o");
  BSONObj o2 = op.getObjectField("o2");
  string ns = op.getStringField("ns");
  size_t i = ns.find('.');
  if (i == string::npos) {
      log() << "oplog format error: invalid namespace '" << ns << "' in op " << op << "." << endl;
  }
  
  string dbname = ns.substr(0, i);
  // string collname = ns.substr(i + 1);
  const char *opType = op.getStringField("op");
  if (*opType == 'i')
     _t->insert(ns, o);
  else if (*opType == 'u')
     _t->update(ns, o2, o);
  else if (*opType == 'd')
     _t->remove(ns, o);
  else if (*opType == 'c') {
    BSONObj info;
    bool ok = _t->runCommand(dbname, o, info);
    if (!ok) {
        const char *fieldName = o.firstElementFieldName();
        string errmsg = info["errmsg"].str();
        bool isDropIndexes = (strncmp(fieldName, "dropIndexes", sizeof("dropIndexes")) == 0 ||
                              strncmp(fieldName, "deleteIndexes", sizeof("deleteIndexes")) == 0);
        if (((strncmp(fieldName, "drop", sizeof("drop")) == 0 || isDropIndexes) &&
             errmsg == "ns not found") ||
            (isDropIndexes && (errmsg == "index not found" ||
                               errmsg.find("can't find index with key:") == 0))) {
            // This is actually ok.  We don't mind dropping something that's not there.
            LOG(1) << "Tried to replay " << o << ", got " << info << ", ignoring." << endl;
        } else {
            log() << "replay of command " << o << " failed: " << info << endl;
        }
      }
    }
  else if ( *opType == 'n' ) {
  // no op
  }
  _maxOpTimeSynced =op["ts"]._opTime();
}

void OplogPlayer::applyOps2tns(boost::shared_ptr<DBClientConnection> _t, const BSONObj& op, const string& tns) {
  BSONObj o = op.getObjectField("o");
  BSONObj o2 = op.getObjectField("o2");
  const char *opType = op.getStringField("op");
  if (*opType == 'i')
     _t->insert(tns, o);
  else if (*opType == 'u')
     _t->update(tns, o2, o);
  else if (*opType == 'd')
     _t->remove(tns, o);
  else if ( *opType == 'n' ) {
  // no op
  }
}

void _sync::printExtraHelp(ostream& out) {
    out << "MongoDB sync tool." << endl;
    out << "For more infomation, please visit \
    http://duoyun.org/topic/5173d275cbce24580a033bd8\n" << endl;
}

void _sync::printVersion(ostream &out) {
     out << _name << " version 1.6.7" << endl;
}

void _sync::SplitString(const string& str,  const string& delimiter, vector<string> &vec) {
  string::size_type pos = 0, pre_pos = 0;
  while ((pos = str.find(delimiter, pos)) !=string::npos) {
    vec.push_back(str.substr(pre_pos, pos-pre_pos));
    pre_pos=++pos;
  }
  vec.push_back(str.substr(pre_pos));  //  process the last columm
}

BSONElement _sync::getTsFromParam(BSONObjBuilder & bb, const string& optimestring) {
  vector<string> vec;
  SplitString(optimestring , ",", vec);
  unsigned long long optime = strtoull(vec[0].c_str(), reinterpret_cast<char **>(NULL), 10);
  unsigned int inc = atoi(vec[1].c_str());
  bb.appendTimestamp("ts", optime, inc);
  vec.clear();
  return bb.done().getField("ts");
}

BSONObj _sync::getlastOp(boost::shared_ptr<DBClientConnection> _s) {
     return _s->findOne(oplogName, Query().sort("$natural", -1), 0, QueryOption_SlaveOk);
}

void _sync::setOplogName(boost::shared_ptr<DBClientConnection> _s) {
  BSONObj isMaster;
  _s->runCommand("admin" , BSON("isMaster" << 1) , isMaster);
  if (isMaster.hasField("hosts")) {  // if connected to replica set member
      oplogName = "local.oplog.rs";
  } else {
      oplogName = "local.oplog.$main";
      if (!isMaster["ismaster"].trueValue()) {
          log() << "oplog mode is only supported on master or replica set member" << endl;
      }
  }
}

string _sync::getOplogName(boost::shared_ptr<DBClientConnection> _s) {
   return oplogName;
}

bool _sync::connectAuth(boost::shared_ptr<DBClientConnection> p,
                   const string &hostname, const string &dbname,
                   const string &username, const string &pwd, string &errmsg) {
    try {
             p->connect(hostname, errmsg);
             log() << hostname << " connected ok" << endl;
             if (!pwd.empty()) {
             bool ok = p->auth(dbname, username, pwd, errmsg);
             if(ok) {
               log() << dbname << " auth ok" << endl;
              } else {
                 log() << dbname << " auth fails" <<endl;
                 return false;
              }
             }
        } catch(DBException &e) {
             log() << "caught " << e.what() << endl;
             return false;
        }
   return true;
}

bool _sync::isTokuMX(boost::shared_ptr<DBClientConnection> _s) {
    //get buildInfo
    BSONObj buildInfo = _s->findOne("admin.$cmd" , BSON("buildInfo" << 1)); 
    if(buildInfo.hasField("tokumxVersion"))
      return true;
     else
      return false;
}

bool _sync::isSharding(boost::shared_ptr<DBClientConnection> _t) {
    BSONObj isdbgrid;
    _t->simpleCommand("admin", &isdbgrid, "isdbgrid");
    return isdbgrid["isdbgrid"].trueValue();
}

bool _sync::createCollection(boost::shared_ptr<DBClientConnection> _t,
                          string ns, const BSONObj& options) {
    long long size = options.getField("size").numberLong();
    bool capped = false;
    int max = 0;
    if( options["capped"].trueValue() ) {
            capped = true;
            BSONElement e = options.getField("max");
            if ( e.isNumber() ) {
                max = e.numberInt();
            }
        }                        
    bool ok = _t->createCollection(ns, size, capped, max);
    return ok; 
}

void _sync::cloneCollection(boost::shared_ptr<DBClientConnection> _s,
                        boost::shared_ptr<DBClientConnection> _t,string sns,string tns)
{
   log() << "cloning " << sns << " -> " << tns << endl;
   int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
   ProgressMeter m(_s->count(sns, _query, queryOptions));
   m.setName("Cloning Progress");
   m.setUnits("objects");
   ProgressMeter* _m=&m;
   Query q = _query;  
   if ( _query.isEmpty() ) 
      q.snapshot();
   auto_ptr<DBClientCursor> c = _s->query(sns, q, 0, 0, 0, queryOptions);
   
   while( c->more() ) {
    BSONObj obj = c->next();
    _t->insert(tns,obj);
     // if there's a progress bar, hit it
        if (_m) {
            _m->hit();
        }
   }
   log() << "\t\t " << m.done() << " objects" << endl;
}
   
void _sync::batchCloneCollection(boost::shared_ptr<DBClientConnection> _s,
                            boost::shared_ptr<DBClientConnection> _t,
                            string sns, string tns) {
    log() << "cloning " << sns << " -> " << tns << endl;
    int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
    ProgressMeter m(_s->count(sns, _query, queryOptions));
    m.setName("Cloning Progress");
    m.setUnits("objects");
    ProgressMeter* _m=&m;
    Query q = _query;
    vector<BSONObj> _insertBuf;
    int insertSize = 0;
    if ( _query.isEmpty() )
       q.snapshot();
    auto_ptr<DBClientCursor> c = _s->query(sns, q, 0 , 0 , 0 , queryOptions);

    while ( c->more() ) {
     BSONObj obj = c->next();
     if ( insertSize + obj.objsize() > _bufferSize ) {
     _t->insert(tns, _insertBuf);
     _insertBuf.clear();
     insertSize = 0;
     }
     _insertBuf.push_back(obj.getOwned());
     insertSize += obj.objsize();
      // if there's a progress bar, hit it
         if (_m) {
             _m->hit();
         }
    }

    if ( !_insertBuf.empty() ) {
    _t->insert(tns, _insertBuf);// last batch
     _insertBuf.clear();
    }
    log() << "\t\t " << m.done() << " objects" << endl;
}

void _sync::clonedb(boost::shared_ptr<DBClientConnection> _s,
                    boost::shared_ptr<DBClientConnection> _t,
                    const string db) {
  log() << "DATABASE: " << db << "\t to \t" << db << endl;
  map <string, BSONObj> collectionOptions;
  vector <string> collections;
  int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
  string ns = db + ".system.namespaces";
  string nsUser = db + ".system.users";
  string nsIndex = db + ".system.indexes";
  
  auto_ptr<DBClientCursor> cursor = _s->query(ns.c_str() , Query() , 0 , 0 , 0 , queryOptions);
  while ( cursor->more() ) {
    BSONObj obj = cursor->nextSafe();
    const string name = obj.getField("name").valuestr();
    if (obj.hasField("options")) {
        collectionOptions[name] = obj.getField("options").embeddedObject().getOwned();
    }

    // skip namespaces with $ in them only if we don't specify a collection to dump
    if (_coll == "" && name.find(".$") != string::npos) {
        LOG(1) << "skipping collection: " << name << endl;
        continue;
    }


    // if a particular collections is specified, and it's not this one, skip it
    if ( _coll != "" && db + "." + _coll != name && _coll != name )
        continue;


    // raise error before writing collection with non-permitted filename chars in the name
    size_t hasBadChars = name.find_first_of("/\0");
    if (hasBadChars != string::npos) {
      error() << "Cannot clone "  << name << ". Collection has '/' or null in the collection name." << endl;
      continue;
    }

    // Don't clone system.indexes or system.users
    if ( endsWith(name.c_str(), ".system.indexes") || endsWith(name.c_str(), ".system.users")) {
      continue;
    }

    collections.push_back(name);
  }

  for (vector<string>::iterator it = collections.begin(); it != collections.end(); ++it) {
      string name = *it;
      if(createCollection(_t, name, collectionOptions[name]))
        LOG(1) << "create collection " << name << ' ' << collectionOptions[name] << endl;
      batchCloneCollection(_s, _t, name, name);
  }
  
  //  process system collections
  if ( _coll.empty() ) {//  -c is not specified
    cloneCollection(_s, _t, nsUser, nsUser);
    cloneCollection(_s, _t, nsIndex, nsIndex);
  } else if (_coll == "system.users") { //sync singe collection system.users
     cloneCollection(_s, _t, nsUser, nsUser);
  } else if (_coll == "system.indexes") {//sync singe collection system.indexes
    cloneCollection(_s, _t, nsIndex, nsIndex);
  } else { // build index when syncing singele collection , -c is specified and collection is not system.users/indexes
      auto_ptr<DBClientCursor> cursorIndex = _s->query(nsIndex.c_str(), BSON("ns" << db + "." + _coll) , 0, 0, 0, queryOptions);
      while ( cursorIndex->more() ) {
        _t->insert(nsIndex, cursorIndex->next());
      }
  }
}

bool _sync::isStale(boost::shared_ptr<DBClientConnection> _s, BSONObj& lastOp, BSONObj& remoteOldestOp) {
     remoteOldestOp = _s->findOne(oplogName, Query());
     OpTime remoteTs = remoteOldestOp["ts"]._opTime();
     OpTime lastTs = lastOp["ts"]._opTime();
      if (lastTs >= remoteTs)
          return false;
      else
          return true; 
}

bool _sync::hasNewOps(boost::shared_ptr<DBClientConnection> _s, const OpTime& lastOpTime) {
     BSONObj remoteNewestOp = getlastOp(_s);
     OpTime remoteNewestTs = remoteNewestOp["ts"]._opTime();
      if (remoteNewestTs > lastOpTime)
          return true;
      else
          return false;  
}

/*
bool _sync::isDelay(boost::shared_ptr<DBClientConnection> _s, BSONObj& op) {
     BSONObj remoteNewestOp = getlastOp(_s);
     OpTime remoteNewestTs = remoteNewestOp["ts"]._opTime();
     OpTime opts = op["ts"]._opTime();
      if ( opts.getSecs() + _delay > remoteNewestTs.getSecs())
          return true;
      else
          return false;  
}*/

void _sync::handleDelay(const BSONObj& lastOp) {
        int sd = _delay;
        // ignore slaveDelay if the box is still initializing. once
        // it becomes secondary we can worry about it.
        if( sd ) {
            const OpTime ts = lastOp["ts"]._opTime();
            long long a = ts.getSecs();
            long long b = time(0);
            long long lag = b - a;
            long long sleeptime = sd - lag;
            if( sleeptime > 0 ) {
                if( sleeptime < 60 ) {
                    sleepsecs((int) sleeptime);
                }
                else {
                    log() << "delay sleep long time: " << sleeptime << endl;;
                    // sleep(hours) would prevent reconfigs from taking effect & such!
                    long long waitUntil = b + sleeptime;
                    while( 1 ) {
                        sleepsecs(6);
                        if( time(0) >= waitUntil )
                            break;

                        //if( theReplSet->myConfig().slaveDelay != sd ) // reconf
                            //break;
                    }
                }
            }
        } // endif slaveDelay
    }

void _sync::report(boost::shared_ptr<DBClientConnection> _s) {
     const OpTime &maxOpTimeSynced = _player->maxOpTimeSynced();
     LOG(0) << "synced up to " << _ptrOpTime->fmtOpTime(maxOpTimeSynced);

     Query lastQuery;
     lastQuery.sort("$natural", -1);
     BSONObj lastFields = BSON("ts" << 1);
     BSONObj lastObj = _s->findOne(oplogName, lastQuery, &lastFields);
     BSONElement tsElt = lastObj["ts"];
     if (!tsElt.ok()) {
         warning() << "couldn't find last oplog entry on remote host" << endl;
         LOG(0) << endl;
         return;
     }
     OpTime lastOpTime = OpTime(tsElt.date());
     LOG(0) << ", source has up to " << _ptrOpTime->fmtOpTime(lastOpTime);
     if (maxOpTimeSynced == lastOpTime) {
         LOG(0) << ", fully synced." << endl;
     }
     else {
         int diff = lastOpTime.getSecs() - maxOpTimeSynced.getSecs();
         if (diff > 0) {
             LOG(0) << ", " << (lastOpTime.getSecs() - maxOpTimeSynced.getSecs())
                    << " seconds behind source." << endl;
         }
         else {
             LOG(0) << ", less than 1 second behind source." << endl;
         }
     }
     _reportingTimer.reset();
 }
    
void _sync::syncCollection(boost::shared_ptr<DBClientConnection> _s,
                           boost::shared_ptr<DBClientConnection> _t,
                           string sns, string tns, const OpTime& optimestart) {
  long long num = 0, cnt = 0;
  int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
  BSONObj query;
  vector<string> vec;
  query = BSON("ts" << GT << optimestart << "ns" << sns);
  num = _s->count(oplogName, query, queryOptions);
  ProgressMeter m(num);
  m.setName("Syncing Progress");
  m.setUnits("ops");
  ProgressMeter* _m=&m;

  log() << "begin to apply oplog..." << endl;
  log() << sns << ":" << num <<" rows oplog to apply..." << endl;

  // if no new oplogs ,waiting.
  while (num == 0) {
    log() << "No new oplogs,the data is the newest ^_^.waiting." << endl;
    sleepsecs(1);
    num = _s->count(oplogName, query, queryOptions);
    if (num > 0)
     break;
  }
  BSONElement latestTs;
  BSONObj op;
  while (true) {
    auto_ptr<DBClientCursor> cursor =
        _s->query(oplogName, query, 0, 0, 0,
                    queryOptions | QueryOption_CursorTailable | QueryOption_AwaitData);

    while ( true ) {
      if ( !cursor->more() ) {
        if (cursor->isDead()) {
            // this sleep is important for collections that start out with no data
            sleepsecs(1);
            break;
        }
        log() << "\t\t " << m.done() << " ops" << endl;
        log() <<  tns << ":" << cnt << " rows oplog are applied,\
        waiting for new data ^_^.The latest optime is " << latestTs.toString() << endl;
        continue;  //  we will try more() again
      }
      // apply oplog
      op = cursor->next();
      latestTs = op.getField("ts");
      _player->applyOps2tns(_t, op, tns);
      ++cnt;
      if (_reportingTimer.seconds() >= _reportingPeriod) {
        log() << "\t" << "Synced up to optime:" << latestTs.toString() << endl;
        _reportingTimer.reset();
      }
      // if there's a progress bar, hit it
      if (_m) {
          _m->hit();
      }
    }  // end inner while loop
  }  // end outer while loop
}

void _sync::syncCollectionRange(boost::shared_ptr<DBClientConnection> _s,
                                boost::shared_ptr<DBClientConnection> _t,
                                string sns, string tns,
                                const OpTime& optimestart,
                                const OpTime& optimestop) {
  int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
  BSONObj query = BSON("ns" << sns << "ts" << GT << optimestart << LTE << optimestop);
  ProgressMeter m(_s->count(oplogName, query, queryOptions));
  m.setName("Cloning Progress");
  m.setUnits("ops");
  ProgressMeter* _m=&m;

  log() << "begin to apply oplog..." << endl;
  log() << sns << "->" << tns << endl;

  auto_ptr<DBClientCursor> cursor =
      _s->query(oplogName, query, 0, 0, 0, queryOptions);
  BSONObj op;
  while ( cursor->more() ) {
    // apply oplog
    op = cursor->next();
    _player->applyOps2tns(_t, op, tns);

    // if there's a progress bar, hit it
    if (_m) {
        _m->hit();
    }
  }  // end  while loop
  log() << "\t\t " << m.done() << " ops" << endl;
}

void _sync::syncdb(boost::shared_ptr<DBClientConnection> _s,
                   boost::shared_ptr<DBClientConnection> _t,
                   const string db , const OpTime& optimestart) {
  log() << "sync database: " << db << endl;
  long long num = 0, cnt = 0;
  BSONObj query;
  string strdb = "^"+ db +"\\..";
  query = BSON("ts" << GT << optimestart << "ns" << BSON("$regex" << strdb));
  num = _s->count(oplogName, query, QueryOption_SlaveOk);
  ProgressMeter m(num);
  m.setName("Syncing Database Progress");
  m.setUnits("ops");
  ProgressMeter* _m=&m;

  log() << "begin to apply oplog..." << endl;
  log() << db << ":" << num << " rows oplog to apply..." << endl;

  // if no new oplogs ,waiting.
  while (num == 0) {
    log() << "No new oplogs,the data is the newest ^_^.waiting." << endl;
    sleepsecs(1);
    num = _s->count(oplogName, query, QueryOption_SlaveOk);
    if (num > 0)
     break;
  }
  BSONElement latestTs;
  BSONObj op;
  while (true) {
      auto_ptr<DBClientCursor> cursor =
        _s->query(oplogName, query, 0, 0, 0,
                  QueryOption_CursorTailable | QueryOption_AwaitData | QueryOption_SlaveOk);

      while ( true ) {
        if ( !cursor->more() ) {
            if ( cursor->isDead() ) {
                // this sleep is important for collections that start out with no data
                sleepsecs(1);
                break;
            }
            log() << "\t\t " << m.done() << " ops" << endl;
            log() << db << ":" << cnt << " rows oplog are applied, \
            waiting for new data ^_^.The latest optime is " << latestTs.toString() << endl;
            continue;  // we will try more() again
        }

        op = cursor->next();
        latestTs = op.getField("ts");
        _player->applyOps(_t, op);
        ++cnt;

        if (_reportingTimer.seconds() >= _reportingPeriod) {
        log() << "\t" << "Synced up to optime:" << latestTs.toString() << endl;
        _reportingTimer.reset();
        }
         // if there's a progress bar, hit it
          if (_m) {
              _m->hit();
          }
      }  //  end inner while loop
  }  //  end outer while loop
}

void _sync::syncdbRange(boost::shared_ptr<DBClientConnection> _s,
                        boost::shared_ptr<DBClientConnection> _t,
                        const string db,
                        const OpTime& optimestart, const OpTime& optimestop) {
  log() << "sync database: " << db << "\t to \t" << db << endl;
  long long cnt = 0;
  BSONElement latestTs;
  string strdb="^"+ db +"\\..";
  BSONObj query = BSON("ns" << BSON("$regex" << strdb) << "ts" << GT << optimestart << LTE << optimestop);
  ProgressMeter m(_s->count(oplogName, query, QueryOption_SlaveOk));
  m.setName("Syncing Database Progress");
  m.setUnits("ops");
  ProgressMeter* _m=&m;

  log() << "begin to apply oplog..." << endl;

  auto_ptr<DBClientCursor> cursor =
      _s->query(oplogName, query, 0, 0, 0, QueryOption_SlaveOk);
  BSONObj op;
  while ( cursor->more() ) {
    op = cursor->next();
    latestTs = op.getField("ts");
    _player->applyOps(_t, op);
    ++cnt;

    if (_reportingTimer.seconds() >= _reportingPeriod) {
      log() << "\t" << "Synced up to optime:" << latestTs.toString() << endl;
      _reportingTimer.reset();
    }
     // if there's a progress bar, hit it
      if (_m) {
          _m->hit();
      }
  }// end  while loop
  log() << "\t\t " << m.done() << " ops" << endl;
}

void _sync::syncAllGTE(boost::shared_ptr<DBClientConnection> _s,
                       boost::shared_ptr<DBClientConnection> _t,
                       const OpTime& optimestart) {
  int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
  BSONObj query;
  query = BSON("ts" << GTE << optimestart);
 
  // if no new oplogs ,waiting.
  while (true) {
    if (hasNewOps(_s, optimestart)) {
      log() << "begin to apply oplog..." << endl;
      break;
    } else {
      log() << "No new ops,waiting..." << endl;
      sleepsecs(5);
    }
  }
  
  BSONObj op;
  while (true) {
    auto_ptr<DBClientCursor> cursor =
         _s->query(oplogName, query, 0, 0, 0,
                   queryOptions | QueryOption_CursorTailable | QueryOption_AwaitData);

    while ( true ) {
       if ( !cursor->more() ) {
           if ( cursor->isDead() ) {
               // this sleep is important for collections that start out with no data
               sleepsecs(1);
               break;
           }
         
           if (_reportingTimer.seconds() >= _reportingPeriod) {
           report(_s);
           log() << "waiting for new data..." << endl; 
           }
           continue;  // we will try more() again
       }
    // apply oplog
    op = cursor->next();
    handleDelay(op);
    
    /*
      // if is delay ,waiting.
    while (true) {
      if (isDelay(_s, op)) {
        log() << "delay, waiting..." << endl; 
        sleepsecs(5);
      } else {
        break;    
      }
    }
    */
    
    _player->applyOps(_t, op);
 
    if (_reportingTimer.seconds() >= _reportingPeriod) {
      report(_s);
    }
    }  // end inner while loop
    //  log() << "\t\t " << m.done() << " ops" << endl;
  }  // end outer while loop
}

void _sync::syncAllRange(boost::shared_ptr<DBClientConnection> _s,
                         boost::shared_ptr<DBClientConnection> _t,
                         const OpTime& optimestart, const OpTime& optimestop) {
  long long cnt = 0;
  int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
  BSONObj query;
  query = BSON("ts" << GTE << optimestart << LTE << optimestop);
  ProgressMeter m(_s->count(oplogName, query, queryOptions));
  m.setName("Syncing Progress");
  m.setUnits("ops");
  ProgressMeter* _m=&m;

  log() << "begin to apply oplog..." << endl;

  auto_ptr<DBClientCursor> cursor =
    _s->query(oplogName, query, 0, 0, 0, queryOptions);
  BSONElement latestTs;
  BSONObj op;
  while ( cursor->more() ) {
    op = cursor->next();
    latestTs = op.getField("ts");
    _player->applyOps(_t, op);
    ++cnt;
    if (_reportingTimer.seconds() >= _reportingPeriod) {
      log() << "\t" << "Synced up to optime:" << latestTs.toString() << endl;
      _reportingTimer.reset();
    };
    // if there's a progress bar, hit it
    if (_m) {
        _m->hit();
    }
  }  // end  while loop
  log() << "\t\t " << m.done() << " ops" << endl;
}

void _sync::go(boost::shared_ptr<DBClientConnection> _s,
               boost::shared_ptr<DBClientConnection> _t,
               string threadName) {
    setThreadName(threadName.c_str());
    setOplogName(_s);
    BSONObj lastOp;
    lastOp = getlastOp(_s);
    OpTime lastTs = lastOp["ts"]._opTime();
    log() << "lastOp OpTime:" << _ptrOpTime->fmtOpTime(lastTs) << endl;

     if (!hasParam("db")) {  // all dbs
        if (!hasParam("optimestart")) {  // no optimestart,clone all dbs
        log() << "clone all dbs ^_^" << endl;
        BSONObj res = _s->findOne("admin.$cmd" , BSON("listDatabases" << 1));
        if (!res["databases"].isABSONObj()) {
            error() << "output of listDatabases isn't what we expected,\
             no 'databases' field:\n" << res << endl;
        }

        BSONObj dbs = res["databases"].embeddedObjectUserCheck();
        set<string> keys;
        dbs.getFieldNames(keys);
        for ( set<string>::iterator i = keys.begin() ; i != keys.end() ; i++ ) {
            string key = *i;

            if (!dbs[key].isABSONObj()) {
                error() << "database field not an object key: " << key << " value: " << dbs[key] << endl;
            }

            BSONObj dbobj = dbs[key].embeddedObjectUserCheck();

            const char * dbName = dbobj.getField("name").valuestr();
            if ( (string)dbName == "local" ||(string)dbName == "admin" )
                continue;

            clonedb(_s, _t, dbName);
        }
      }

        if (hasParam("oplog")) {
          if (hasParam("optimestart") && hasParam("optimestop")) {
            syncAllRange(_s, _t, *_opTimeStart.get(), *_opTimeStop.get());
          } else if (hasParam("optimestart")) {
            syncAllGTE(_s, _t, *_opTimeStart.get());
          } else {  // clone+inc sync
              BSONObj remoteOldestOp;
              if(isStale(_s, lastOp, remoteOldestOp)) {
                OpTime remoteTs = remoteOldestOp["ts"]._opTime();
                log() << "error,too stale to catch up" <<endl;
                log() << "replSet remoteOldestOp:    " << remoteTs.toStringLong() << endl;
                log() << "replSet lastOpTime:   " << lastTs.toString() << endl;
              } else {
                log() << "clone done,start to catch up" << endl;
                syncAllGTE(_s, _t, lastTs);
              }
          }
      }
    } else {  // has db parameter
    string sns, tns;
    sns = getParam("db")+"."+getParam("collection");
    if (hasParam("tns"))
      tns = getParam("tns");
    else
      tns = sns;
    if (!hasParam("optimestart")) {  // optimestart is only for incremental oplog sync
      // clone data
      if (hasParam("tns"))
        batchCloneCollection(_s, _t, sns, tns);
      else
        clonedb(_s, _t, _db);
    }

    if (hasParam("oplog")) {
        // oplog sync
        if (hasParam("optimestart") && hasParam("optimestop")) {
          if (hasParam("collection"))
           syncCollectionRange(_s, _t, sns, tns, *_opTimeStart.get(), *_opTimeStop.get());
         else
           syncdbRange(_s, _t, _db, *_opTimeStart.get(), *_opTimeStop.get());
        } else if (hasParam("optimestart")) {
         if ( hasParam("collection"))
           syncCollection(_s, _t, sns, tns, *_opTimeStart.get());
         else
           syncdb(_s, _t,  _db, *_opTimeStart.get());
       } else {  // clone+inc syncs
          if ( hasParam("collection"))
            syncCollection(_s, _t, sns, tns, lastTs);
          else
            syncdb(_s, _t, _db, lastTs);
       }
      }
    }
}

void _sync::goSharding2Single(boost::shared_ptr<DBClientConnection> _s, boost::shared_ptr<DBClientConnection> _t) {
       boost::thread_group grp;
      string errmsg;
      log() << "stop balancer" << endl;
      _s->update("config.settings", QUERY("_id" << "balancer"), BSON("$set" << BSON("stopped" << true)), false, true);

      auto_ptr<DBClientCursor> c = _s->query("config.shards", Query().sort("_id", 1) );
      vector<BSONObj> sourceShards;
      while ( c->more() ) {
          sourceShards.push_back(c->next().getOwned());
      }
      
      //sync data of each shard
      for ( unsigned i = 0; i < sourceShards.size(); i++ ) {
        BSONObj x = sourceShards[i];
        ConnectionString cs = ConnectionString::parse(x["host"].String() , errmsg);
        if ( errmsg.size() ) {
            cerr << errmsg << endl;
            continue;
        }

        vector<HostAndPort> v = cs.getServers();
         int gotSecondary = 0;
         for ( unsigned i = 0; i < v.size(); i++ ) {
          _s.reset(new DBClientConnection());
          _t.reset(new DBClientConnection());
          string host = v[i].toString();
          bool _sOk = connectAuth(_s, host, "admin", getParam("username"), getParam("password"), errmsg);
          bool _tOk = connectAuth(_t, getParam("to"), "admin", getParam("tu"), _tp, errmsg);
          if(!_sOk || !_tOk) {
             return;
          }
           BSONObj isMaster;
           _s->simpleCommand("admin", &isMaster, "isMaster");
            if (!isMaster["ismaster"].trueValue() && gotSecondary == 0) {//choose one secdary as source
             ++gotSecondary;
             grp.add_thread(new boost::thread(boost::bind(&_sync::go, this, _s, _t, host)));
            }
        }
      }
      grp.join_all();
}

void _sync::goSharding2Sharding(boost::shared_ptr<DBClientConnection> _s, boost::shared_ptr<DBClientConnection> _t) {
      boost::thread_group grp;
      string errmsg;
      log() << "stop balancer" << endl;
      _s->update("config.settings", QUERY("_id" << "balancer"), BSON("$set" << BSON("stopped" << true)), false, true);

      auto_ptr<DBClientCursor> c = _s->query("config.shards", Query().sort("_id", 1) );
      vector<BSONObj> sourceShards;
      while ( c->more() ) {
          sourceShards.push_back(c->next().getOwned());
      }
      
      auto_ptr<DBClientCursor> curTarget = _t->query("config.shards", Query().sort("_id", 1) );
      vector<BSONObj> targetShards;
      while ( curTarget->more() ) {
          targetShards.push_back(curTarget->next().getOwned());
      }
      
      //map shards
      if(targetShards.size() < sourceShards.size()){
        log() << "target shards are less than source shards." << endl;
        return;
      }
      map<string, string> shardNameMap;
      vector<string> vSourceShardName, vTargetShardName;
      for ( unsigned i = 0; i < sourceShards.size(); i++ ) {
        string souceShardName = sourceShards[i].getStringField("_id");
        string targetShardName = targetShards[i].getStringField("_id");
        vSourceShardName.push_back(souceShardName);
        vTargetShardName.push_back(targetShardName);
        shardNameMap.insert( pair<string, string> (souceShardName, targetShardName) );
      }
      
      //get config servers of target sharding.
       BSONObj shardMap, mapinfo;
       vector<string> vConfig;
       bool ok = _t->runCommand("admin",  BSON("getShardMap" << 1), shardMap);
       if(ok) {
        mapinfo = shardMap.getObjectField("map");
        string configServrs = mapinfo["config"].String();
        SplitString(configServrs, ",", vConfig);
       }
      
      //process config data
      boost::shared_ptr<DBClientConnection> _tConfig;
      if(vSourceShardName == vTargetShardName) {//  shard name is the same and hostname:port must be different.
         //  copy config data to config servers
        for ( unsigned i = 0; i < vConfig.size(); i++ ) {
         _tConfig.reset(new DBClientConnection());
         bool _connConfigOk = connectAuth(_tConfig, vConfig[i], "admin", getParam("tu"), _tp, errmsg);
         if(!_connConfigOk)
          return;
        cloneCollection(_s, _tConfig, "config.databases", "config.databases");
        cloneCollection(_s, _tConfig, "config.collections", "config.collections");
        cloneCollection(_s, _tConfig, "config.chunks", "config.chunks");  
        }
      } else { //process config data in target first shard.
        BSONObj t0 = targetShards[0];
        ConnectionString ct0 = ConnectionString::parse(t0["host"].String() , errmsg);
        if ( errmsg.size() ) {
            cerr << errmsg << endl;
        }
        vector<HostAndPort> vt0 = ct0.getServers();
        for ( unsigned i = 0; i < vt0.size(); i++ ) {
          _t.reset(new DBClientConnection());
          string host = vt0[i].toString();
          bool _t0Ok = connectAuth(_t, host, "admin", getParam("tu"), _tp, errmsg);
          if(!_t0Ok) 
          	return;
          BSONObj isMaster;
          _t->simpleCommand("admin", &isMaster, "isMaster");
          if (isMaster["ismaster"].trueValue()) {
             cloneCollection(_s, _t, "config.databases", "mongosync.databases");
             cloneCollection(_s, _t, "config.chunks", "mongosync.chunks");
             //process config data copyied from source shards. 
             map<string, string>::iterator it;
             for( it = shardNameMap.begin(); it != shardNameMap.end(); ++it ) {
              _t->update("mongosync.databases", QUERY("primary" << it->first), BSON("$set" << BSON("primary" << it->second)), false, true);
              _t->update("mongosync.chunks", QUERY("shard" << it->first), BSON("$set" << BSON("shard" << it->second)), false, true);
             }
             
             //  copy config data to config servers
             for ( unsigned i = 0; i < vConfig.size(); i++ ) {
              _tConfig.reset(new DBClientConnection());
              bool _connConfigOk = connectAuth(_tConfig, vConfig[i], "admin", getParam("tu"), _tp, errmsg);
              if(!_connConfigOk)
               return;
             cloneCollection(_t, _tConfig, "mongosync.databases", "config.databases");
             cloneCollection(_s, _tConfig, "config.collections", "config.collections");
             cloneCollection(_t, _tConfig, "mongosync.chunks", "config.chunks");
             }
             //  drop mongosync databases
             _t->dropDatabase("mongosync");
          }
        }
      }
      
      //sync data of each shard
      for ( unsigned i = 0; i < sourceShards.size(); i++ ) {
        BSONObj x = sourceShards[i];
        ConnectionString cs = ConnectionString::parse(x["host"].String() , errmsg);
        if ( errmsg.size() ) {
            cerr << errmsg << endl;
            continue;
        }
        
        BSONObj y = targetShards[i];
        ConnectionString ct = ConnectionString::parse(y["host"].String() , errmsg);
        if ( errmsg.size() ) {
            cerr << errmsg << endl;
            continue;
        }

        vector<HostAndPort> v = cs.getServers();
        vector<HostAndPort> vt = ct.getServers();
        
        for ( unsigned i = 0; i < v.size(); i++ ) {
          _s.reset(new DBClientConnection());
         
          string host = v[i].toString();
         
          bool _sOk = connectAuth(_s, host, "admin", getParam("username"), getParam("password"), errmsg);
    
          if(!_sOk) {
             return;
          }
           BSONObj isMaster;
           _s->simpleCommand("admin", &isMaster, "isMaster");
            if (isMaster["ismaster"].trueValue()) {
              for ( unsigned i = 0; i < vt.size(); i++ ) {
                 _t.reset(new DBClientConnection());  
                string targetHost = vt[i].toString();  //get correct target host.
                bool _tOk = connectAuth(_t, targetHost, "admin", getParam("tu"), _tp, errmsg);
                if(!_tOk) {
                 return;
                }
                BSONObj isTargetMaster;
                _t->simpleCommand("admin", &isTargetMaster, "isMaster");
                if (isTargetMaster["ismaster"].trueValue())
                 grp.add_thread(new boost::thread(boost::bind(&_sync::go, this, _s, _t, host)));
             }
            }
        }
      }
      grp.join_all();
}

void _sync::goTokuMX(boost::shared_ptr<DBClientConnection> _s,
               boost::shared_ptr<DBClientConnection> _t,
               string threadName) {
       setThreadName(threadName.c_str());         
       log() <<"TokuMX incremental sync is not supported" <<endl; 
}
bool _sync::doFork() {
       if (_params.count("fork")) {
        cout.flush();
        cerr.flush();

        cout << "about to fork child process" << endl;

        pid_t child = fork();
        if (child == -1) {
            cout << "ERROR: stage 1 fork() failed: " << errnoWithDescription();
            _exit(EXIT_ABRUPT);
        }  else if (child == 0) {
          cout << "child process started: " << getpid() << endl;
        }  else if (child > 0) {
            // this is run in the original parent process
            int pstat;
            cout << "forked process: " << child << endl;
            waitpid(child, &pstat, WNOHANG);
            cout << "child process started successfully, parent exiting" << endl;
            _exit(50);
        }
        
        fclose(stderr);
        fclose(stdin);

        FILE* f = freopen("/dev/null", "w", stderr);
        if ( f == NULL ) {
            cout << "Cant reassign stderr while forking server process: " << strerror(errno) << endl;
            return false;
        }

        f = freopen("/dev/null", "r", stdin);
        if ( f == NULL ) {
            cout << "Cant reassign stdin while forking server process: " << strerror(errno) << endl;
            return false;
        }
        }
       // setup cwd
       string cwd;
       char buffer[1024];
       verify( getcwd( buffer , 1000 ) );
       cwd = buffer;
       
      if (!getParam("logpath").empty()) {
            string absoluteLogpath = boost::filesystem::absolute(
                    getParam("logpath"), cwd).string();
            if (!initLogging(absoluteLogpath, hasParam("logappend"))) {
                cout << "Bad logpath value: \"" << absoluteLogpath << "\"; terminating." << endl;
                return false;
            }
        }
        
      return true;   
}
int _sync::run() {
    if (!doFork())
        ::_exit(EXIT_FAILURE);
    if (!hasParam("to")) {
        log() << "need to specify --to" << endl;
        return -1;
    }

    if (_params.count("version")) {
        printVersion(cout);
        return 0;
    }
    
    if ( _params.count( "tp" )
        && ( _tp.empty() ) ) {
     _tp = askPassword();
    }

    Client::initThread("mongosync");

    {
      string q = getParam("query");
      if ( q.size() )
        _query = fromjson(q);
    }

    _player.reset(new OplogPlayer());
    
    _ptrOpTime.reset(new OpTimeTool());
    string opTimeStart, opTimeStop;
    if (hasParam("optimestart")) {
    opTimeStart = getParam("optimestart");
    _ptrOpTime->setOptime(opTimeStart, _opTimeStart);
    }
    if (hasParam("optimestop")) {
    opTimeStop = getParam("optimestop");
    _ptrOpTime->setOptime(opTimeStop, _opTimeStop);
    }
    
    _reportingPeriod = getParam("reportingPeriod", 10);
    _delay = getParam("delay", 0);
    _mode = getParam("mode", 0);
    int maxSize = BSONObjMaxUserSize - 1024;
    int bufferSize = getParam("bufferSize", maxSize/1024 ) * 1024;
    _bufferSize = (bufferSize < maxSize ? bufferSize : maxSize) ;
    
    string sourceserver, targetserver, errmsg;

    if ( hasParam("host") && hasParam("port"))
      sourceserver = getParam("host")+string(":")+getParam("port");
    else
      sourceserver = getParam("host");

    targetserver = getParam("to");
    boost::shared_ptr<DBClientConnection> _s(new DBClientConnection());
    boost::shared_ptr<DBClientConnection> _t(new DBClientConnection());
    bool sourceOk = connectAuth(_s, sourceserver, "admin", getParam("username"), getParam("password"), errmsg);
    bool targetOk = connectAuth(_t, targetserver, "admin", getParam("tu"), _tp, errmsg);
    if(!(sourceOk && targetOk)) {
       return -1;
     }
    if(isTokuMX(_s) && hasParam("oplog")) {
      goTokuMX(_s, _t, string("tokumx"));
      return 0;
     }  
     
    _usingMongos = isMongos();
    if (_usingMongos && _query.isEmpty()) {
      if(isSharding(_t) && _mode == 0)
        goSharding2Single(_s, _t); 
      else
        goSharding2Sharding(_s, _t); 
    } else if(!_query.isEmpty()){
      clonedb(_s, _t, _db);
    } else {
         go(_s, _t, string("mongosync"));
    }

  return 0;
}


int main(int argc , char ** argv, char ** envp) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    _sync t;
    return t.main( argc , argv );
}