// @file sync.h

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
#include "mongo/tools/tool.h"
// 2.4 #include "mongo/s/type_shard.h"

#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

using namespace mongo;

namespace po = boost::program_options;

class  OpTimeTool {
    public:
    string fmtOpTime(const OpTime &t);
    void setOptime( const string &optime, boost::scoped_ptr<OpTime>& _ptr );
};

class OplogPlayer {
  OpTime _maxOpTimeSynced;
  public:
    void applyOps(boost::shared_ptr<DBClientConnection> _t, const BSONObj& op);
    void applyOps2tns(boost::shared_ptr<DBClientConnection> _t, const BSONObj& op, const string& tns);
    void applyOpsTokuMX(boost::shared_ptr<DBClientConnection> _t, const BSONObj& op);  
    const OpTime &maxOpTimeSynced() const { return _maxOpTimeSynced; }
};

class _sync : public Tool {
  public:
    _sync() : Tool("sync" ), _reportingTimer() {
        addFieldOptions();
        add_options()
        ("to", po::value<string>() , "target host:port" )
        ("tu", po::value<string>() , "target username" )
        ("tp", new PasswordValue( &_tp ), "target password" )
        ("tns", po::value<string>() , "--to ns,rename to a new collection" )
        ("query,q", po::value<string>() , "json query" )
        ("oplog", "Use oplog for point-in-time sync")
        ("optimestart,s" , po::value<string>() , "optime start timestamp from rs.status(), use in conjunction with --oplog option" )
        ("optimestop,t" , po::value<string>() , "optime stop timestamp, use in conjunction with --oplog -s option" )
        ("delay", po::value<int>() , "seconds delay" ) 
        ("mode", po::value<int>()->default_value(0) , "shards sync mode,default 0,0=shard2mongos,1=shard2shard" ) 
        ("fork", "fork mongosync process" )  
        ("logpath", po::value<string>() , "log file to send write to instead of stdout - has to be a file, not directory" )
        ("logappend" , "append to logpath instead of over-writing" )
        ("bufferSize", po::value<int>() , "bufferSize KB when cloning,1~16384" )  
        ("reportingPeriod", po::value<int>(&_reportingPeriod)->default_value(10) , "seconds between progress reports" )
        ;
    }

    virtual void printExtraHelp(ostream& out);
    virtual void printVersion(ostream &out);
    void SplitString(const string& str, const string& delimiter, vector<string>& vec);

    BSONObj getlastOp(boost::shared_ptr<DBClientConnection> _s);
    void setOplogName(boost::shared_ptr<DBClientConnection> _s);  
    string getOplogName(boost::shared_ptr<DBClientConnection> _s);
    BSONElement getTsFromParam(BSONObjBuilder & bb, const string& optimestring);

    bool connectAuth(boost::shared_ptr<DBClientConnection> p,
                     const string &hostname, const string &dbname,
                     const string &username, const string &pwd, string &errmsg);
                     
    bool isTokuMX(boost::shared_ptr<DBClientConnection> _s); 
    bool isSharding(boost::shared_ptr<DBClientConnection> _t);                 
                     
    bool createCollection(boost::shared_ptr<DBClientConnection> _t,
                          string ns, const BSONObj& options);  
    void cloneCollection(boost::shared_ptr<DBClientConnection> _s,
                            boost::shared_ptr<DBClientConnection> _t,string sns,string tns);
                              
    void batchCloneCollection(boost::shared_ptr<DBClientConnection> _s,
                         boost::shared_ptr<DBClientConnection> _t,
                         string sns, string tns);

    void clonedb(boost::shared_ptr<DBClientConnection> _s,
                 boost::shared_ptr<DBClientConnection> _t, const string db);
                  
    bool isStale(boost::shared_ptr<DBClientConnection> _s, BSONObj& lastOp, BSONObj& remoteOldestOp); 
      
    bool hasNewOps(boost::shared_ptr<DBClientConnection> _s, const OpTime& lastOpTime); 
      
   // bool isDelay(boost::shared_ptr<DBClientConnection> _s, BSONObj& op);
    void handleDelay(const BSONObj& lastOp);
    
    void report(boost::shared_ptr<DBClientConnection> _s);
    
    void syncCollection(boost::shared_ptr<DBClientConnection> _s,
                        boost::shared_ptr<DBClientConnection> _t, string sns,
                        string tns, const OpTime& optimestart);

    void syncCollectionRange(boost::shared_ptr<DBClientConnection> _s,
                             boost::shared_ptr<DBClientConnection> _t,
                             string sns, string tns,
                             const OpTime& optimestart, const OpTime& optimestop);

    void syncdb(boost::shared_ptr<DBClientConnection> _s,
                boost::shared_ptr<DBClientConnection> _t,
                const string db , const OpTime& optimestart);

    void syncdbRange(boost::shared_ptr<DBClientConnection> _s,
                     boost::shared_ptr<DBClientConnection> _t,
                     const string db ,
                     const OpTime& optimestart, const OpTime& optimestop);

    void syncAllGTE(boost::shared_ptr<DBClientConnection> _s,
                    boost::shared_ptr<DBClientConnection> _t, const OpTime& optimestart);

    void syncAllRange(boost::shared_ptr<DBClientConnection> _s,
                      boost::shared_ptr<DBClientConnection> _t,
                      const OpTime& optimestart, const OpTime& optimestop);

    void goMongoDB(boost::shared_ptr<DBClientConnection> _s,
            boost::shared_ptr<DBClientConnection> _t, string threadName);
    void goTokuMX(boost::shared_ptr<DBClientConnection> _s,
            boost::shared_ptr<DBClientConnection> _t, string threadName);
    void go(boost::shared_ptr<DBClientConnection> _s,
            boost::shared_ptr<DBClientConnection> _t, string threadName);          
    void goSharding2Single(boost::shared_ptr<DBClientConnection> _s, boost::shared_ptr<DBClientConnection> _t);          
    void goSharding2Sharding(boost::shared_ptr<DBClientConnection> _s, boost::shared_ptr<DBClientConnection> _t);
    virtual int run();
    bool doFork();

  private:
    bool _usingMongos;
    BSONObj _query;
    string oplogName;
    string _tp;
    scoped_ptr<OplogPlayer> _player;
    scoped_ptr<OpTimeTool> _ptrOpTime;
    scoped_ptr<OpTime> _opTimeStart; 
    scoped_ptr<OpTime> _opTimeStop;
    mutable Timer _reportingTimer;
    int _reportingPeriod;
    int _bufferSize; // real bufferSize
    int _delay;
    int _mode;
};