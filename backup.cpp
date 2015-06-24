// backup.cpp

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

#include <fcntl.h>
#include <map>
#include <fstream>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/convenience.hpp>

#include "mongo/base/initializer.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/db.h"
#include "mongo/db/namespacestring.h"
#include "mongo/util/version.h"
#include "mongo/tools/tool.h"

using namespace mongo;

namespace po = boost::program_options;

class Backup : public BSONTool {
    class FilePtr : boost::noncopyable {
    public:
        /*implicit*/ FilePtr(FILE* f) : _f(f) {}
        ~FilePtr() { fclose(_f); }
        operator FILE*() { return _f; }
    private:
        FILE* _f;
    };
public:
    scoped_ptr<Matcher> _opmatcher; 
    scoped_ptr<OpTime> _opTimeStart; 
    scoped_ptr<OpTime> _opTimeStop; 
    int _oplogEntrySkips; // oplog entries skipped
    int _oplogEntryApplies; // oplog entries applied
    static volatile bool running;
    Backup() : BSONTool( "backup" ) {
        add_options()
        ("out,o", po::value<string>()->default_value("backup"), "output directory or \"-\" for stdout")
        ("backup", "backup" )
        ("optimestart,s", po::value<string>(), "Use optimestart timestamp  for incremental backup/recovery")
        ("stream", "Use stream for real-time incremental backup" ) 
        ("recovery", "recovery")
        ("optimestop,t", po::value<string>(), "Use optimestop timestamp for  incremental recovery")
        ;
        add_hidden_options()
        ("dir", po::value<string>()->default_value("backup"), "directory to recovery from")
        ;
        addPositionArg("dir", 1);
    }

    virtual void preSetup() {
        string out = getParam("out");
        if ( out == "-" ) {
                // write output to standard error to avoid mangling output
                // must happen early to avoid sending junk to stdout
                useStandardOutput(false);
        }
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Incremental backup and recovery tool for MongoDB.\n" << endl;
    }
    
    void printVersion(ostream &out) {
    out << _name << " db version " << mongo::versionString;
    if (mongo::versionString[strlen(mongo::versionString)-1] == '-')
        out << " (commit " << mongo::gitVersion() << ")";
        out <<",mongobackup version 1.0.6";
    out << endl;
    }
    
    void applyOps( const BSONObj& op) {
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
         conn().insert(ns, o);
      else if (*opType == 'u')
         conn().update(ns, o2, o);
      else if (*opType == 'd')
         conn().remove(ns, o);
      else if (*opType == 'c') {
        BSONObj info;
        bool ok = conn().runCommand(dbname, o, info);
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
     }

    // This is a functor that writes a BSONObj to a file
    struct Writer {
        Writer(FILE* out, ProgressMeter* m) :_out(out), _m(m) {}

        void operator () (const BSONObj& obj) {
            size_t toWrite = obj.objsize();
            size_t written = 0;

            while (toWrite) {
                size_t ret = fwrite( obj.objdata()+written, 1, toWrite, _out );
                uassert(140350, errnoWithPrefix("couldn't write to file"), ret);
                toWrite -= ret;
                written += ret;
            }

            // if there's a progress bar, hit it
            if (_m) {
                _m->hit();
            }
        }

        FILE* _out;
        ProgressMeter* _m;
    };

    void doCollection( const string coll , FILE* out , ProgressMeter *m ) {
        Query q = _query;
        BSONObj op,firstOpOfFile;
        int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
               
        DBClientBase& connBase = conn(true);
        m->setTotalWhileRunning( conn( true ).count( coll.c_str() , BSONObj() , queryOptions ) );
        Writer writer(out, m);
        if ( hasParam("stream") ) {
            queryOptions |= QueryOption_CursorTailable | QueryOption_AwaitData;
            boost::filesystem::path root( getParam("out") );
           
            int seq=0;
            // if resume backup, read file seqence from metadata file.
            boost::filesystem::path metadataFile = (root / "oplog.metadata.json" );
            if ( exists(metadataFile) ) {
              map<string, BSONObj> OplogMetadata;
              parseOplogMetadataFile(metadataFile.string(), OplogMetadata);  
              seq = atoi((OplogMetadata.rbegin()->first).c_str()) + 1; // maybe use stoi when in c++ 11
            }
            
            stringstream sseq; 
            sseq<< setw(6) << setfill('0') << seq;  
            string oplogfilename = "oplog" + sseq.str() + ".bson";
            boost::filesystem::path path(root / oplogfilename);
            FILE *f = fopen(path.string().c_str(), "wb");
            scoped_ptr<Writer> writer_ptr(new Writer(f, m));  
            scoped_ptr<boost::filesystem::path> path_ptr(new boost::filesystem::path (root / oplogfilename));  
            vector<BSONElement> _v;
            
            //write the first timestamp to metadata.
            while ( true ) {
              scoped_ptr<DBClientCursor> cur(connBase.query( coll.c_str() , q , 1 , 0 , 0 , queryOptions )); 
              if (cur->more()) {
                BSONObj firstOp = cur->next(); //getOwnned got "assertion: 13655 BSONElement: bad type 80",but why ? or all be getOwnned.
                _v.push_back( firstOp.getField("ts"));
                break;
              } else {
                  log() << "\t" << "no ops, waiting..." << endl;
                  sleepsecs(1);
                  continue;// we will try again
              }
            }
            
            while ( running ) {
            scoped_ptr<DBClientCursor> cursor(connBase.query( coll.c_str() , q , 0 , 0 , 0 , queryOptions ));

               while ( running ) {
               if ( !(cursor.get() && cursor->more()) ) {
                 if ( cursor->isDead() ) {
                     // this sleep is important for collections that start out with no data
                     sleepsecs(1);
                     break;
                 }
                 
                 log() << "\t" << " waiting for new ops ^_^" << endl;
                 //  sleepsecs(1);  //  five seconds interval when no sleep
                 continue;// we will try more() again
               }
                op = cursor->next();
                (*writer_ptr)(op);
                  
                if (file_size(*path_ptr) >= 1073741824) {
                  fclose(f);  
                  _v.push_back( op.getField("ts"));
                  writeOplogMetadataFile(  root / (string("oplog") + ".metadata.json"), sseq.str(), _v );
                  
                  //  rotate to new file
                  ++seq;
                  sseq.str("");
                  sseq<< setw(6) << setfill('0') << seq;
                  oplogfilename = "oplog" + sseq.str() + ".bson";
                  path_ptr.reset(new boost::filesystem::path (root / oplogfilename));
                  f = fopen((*path_ptr).string().c_str(), "wb");
                  writer_ptr.reset(new Writer(f, m));
                  
                  while ( true ) {
                    if ( cursor->more() ) {
                    _v.clear();
                    firstOpOfFile = (cursor->next()).getOwned();//It's important to use getOwned!
                    (*writer_ptr)(firstOpOfFile);
                    _v.push_back( firstOpOfFile.getField("ts"));
                     break;
                    } else {
                       log() << "\t" << " waiting for new ops too ^_^" << endl;
                       sleepsecs(1);
                       continue;// we will try more() again
                    }
                  }
                }
              
               }// end inner while loop
             log() << "\t\t " << m->done() << " objects" << endl;
            }// end outer while loop
            
            // exit
            log() <<  "Backuped up to " << op.getField("ts").toString() << endl;
            log() <<  "Use -s " << op.getField("ts")._opTime().getSecs() << "," 
            << op.getField("ts")._opTime().getInc()<< " to resume." << endl;
            fclose(f);  //  close the file which is being written.
            _v.push_back( op.getField("ts"));
            writeOplogMetadataFile(  root / (string("oplog") + ".metadata.json"), sseq.str(), _v );
        }
        // use low-latency "exhaust" mode if going over the network
        else if (!_usingMongos && typeid(connBase) == typeid(DBClientConnection&)) {
            DBClientConnection& conn = static_cast<DBClientConnection&>(connBase);
            boost::function<void(const BSONObj&)> castedWriter(writer); // needed for overload resolution
            conn.query( castedWriter, coll.c_str() , q , NULL, queryOptions | QueryOption_Exhaust);
        }
        else {
            //This branch should only be taken with DBDirectClient or mongos which doesn't support exhaust mode
            scoped_ptr<DBClientCursor> cursor(connBase.query( coll.c_str() , q , 0 , 0 , 0 , queryOptions ));
            while ( cursor->more() ) {
                writer(cursor->next());
            }
        }
    }

    void writeCollectionFile( const string coll , boost::filesystem::path outputFile ) {
        log() << "\t" << coll << " to " << outputFile.string() << endl;

        FilePtr f (fopen(outputFile.string().c_str(), "wb"));
        uassert(102620, errnoWithPrefix("couldn't open file"), f);

        ProgressMeter m(conn(true).count(coll.c_str(), _query, QueryOption_SlaveOk));
        m.setName("Backup Progress");
        m.setUnits("objects");

        doCollection(coll, f, &m);

        log() << "\t\t " << m.done() << " objects" << endl;
    }
    
    void writeOplogMetadataFile( boost::filesystem::path outputFile, string seq, vector<BSONElement> &_v ) {
        log() << "\tMetadata for oplog"<< seq  << " to " << outputFile.string() << endl;
        BSONObjBuilder metadata;
        metadata << "seq" << seq;
        BSONArrayBuilder tsoptime (metadata.subarrayStart("tsOptime"));
        for (vector<BSONElement>::iterator it = _v.begin(); it != _v.end(); ++it) {
          tsoptime << (*it);
        }
        tsoptime.done();
       
        ofstream file (outputFile.string().c_str(),ios::in|ios::app);
        file << metadata.done().jsonString() << endl;
    }
    
    void setOptime( const string &optime, boost::scoped_ptr<OpTime>& _ptr ) {
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

    int doRun() {
        string opLogName = "";
        string opTimeStart, opTimeStop;
        if (hasParam("optimestart")) {
        opTimeStart = getParam("optimestart");
        setOptime(opTimeStart, _opTimeStart);
        }
        if (hasParam("optimestop")) {
        opTimeStop = getParam("optimestop");
        setOptime(opTimeStop, _opTimeStop);
        }
       
        if (hasParam("backup")) {
            if ( hasParam("db") || hasParam("collection")) {
                log() << "mongobackup only support incremental backup." << endl;
                return -1;
            }

            BSONObj isMaster;
            conn("true").simpleCommand("admin", &isMaster, "isMaster");

            if (isMaster.hasField("hosts")) { // if connected to replica set member
                opLogName = "local.oplog.rs";
            }
            else {
                opLogName = "local.oplog.$main";
                if ( ! isMaster["ismaster"].trueValue() ) {
                    log() << "oplog mode is only supported on master or replica set member" << endl;
                    return -1;
                }
            }
            
            // check if we're outputting to stdout
            string out = getParam("out");
            _usingMongos = isMongos();
            
            boost::filesystem::path root( out );
            
            if (!opLogName.empty()) {
                boost::filesystem::create_directories( out );
                if (!opTimeStart.empty()) {
                BSONObjBuilder tsBldr;
                tsBldr << "$gt" << *_opTimeStart.get();
                _query = BSON("ts" << tsBldr.obj() );
                } else {
                  _query = BSONObj();
                }
            
                writeCollectionFile( opLogName , root / "oplog.bson" );
                return 0;
            }

           return 0;
        }
                
        if(hasParam("recovery")) {
          boost::filesystem::path root = getParam("dir");
          if ( is_directory( root ) ) {
            boost::filesystem::path metadataFile = (root / "oplog.metadata.json" );
            if ( exists(metadataFile) ) {
              map<string, BSONObj> OplogMetadata;
              map<string, BSONObj>::iterator it;
              parseOplogMetadataFile(metadataFile.string(), OplogMetadata);  
              OpTime tsOptimeStart, tsOptimeStop;
              
              for( it = OplogMetadata.begin(); it != OplogMetadata.end(); ++it ) {
                tsOptimeStart = it->second.getFieldDotted("tsOptime.0")._opTime();
                tsOptimeStop = it->second.getFieldDotted("tsOptime.1")._opTime();
                string oplogfilename = "oplog" + it->first + ".bson";
                
                //replay oplog file
                if ( tsOptimeStop > *_opTimeStart.get() && tsOptimeStart < *_opTimeStop.get() ) {//interact
                     BSONObjBuilder tsRestrictBldr;
                     if (!(*_opTimeStart.get()).isNull() && !(*_opTimeStop.get()).isNull()) {
                       if ( tsOptimeStart >= *_opTimeStart.get() && tsOptimeStop <= *_opTimeStop.get()) {//included
                         _opmatcher.reset(new Matcher(BSONObj()));
                         log() << "Replaying file:" << oplogfilename << endl;
                         processFile( root / oplogfilename );
                       } else {
                       tsRestrictBldr << "$gte" << ( tsOptimeStart >= *_opTimeStart.get() ? tsOptimeStart : *_opTimeStart.get() );
                       tsRestrictBldr << "$lte" << ( tsOptimeStop <= *_opTimeStop.get() ? tsOptimeStop : *_opTimeStop.get() );
                       
                       BSONObj query = BSON("ts" << tsRestrictBldr.obj());
                       log() << "Replaying file:" << oplogfilename << endl;
                       log() << "Only applying oplog entries matching this criteria: "
                                   << query.jsonString() << endl;
                        _opmatcher.reset(new Matcher(query));
                        processFile( root / oplogfilename );
                       }
                     }
                } else {
                    log() << "Skip file "<< oplogfilename << endl;
                }            
              }
            }  else {
               BSONObjBuilder tsRestrictBldr;
               if (!(*_opTimeStart.get()).isNull() && !(*_opTimeStop.get()).isNull()) {
                     tsRestrictBldr << "$gte" << *_opTimeStart.get();
                     tsRestrictBldr << "$lte" << *_opTimeStop.get();
                       
                     BSONObj query = BSON("ts" << tsRestrictBldr.obj());
                     log() << "Replaying file oplog.bson" << endl;
                     log() << "Only applying oplog entries matching this criteria: "
                                   << query.jsonString() << endl;
                     _opmatcher.reset(new Matcher(query));
                     processFile( root / "oplog.bson" ); 
              }
           }
           log() << "Successfully Recovered." << endl;
          }
        
          return 0;
        }
      return 0;
    }
    
    virtual void gotObject( const BSONObj& obj ) {
            if (obj["op"].valuestr()[0] == 'n') // skip no-ops
                return;
            
            // exclude operations that don't meet (timestamp) criteria
            if ( _opmatcher.get() && ! _opmatcher->matches ( obj ) ) {
                _oplogEntrySkips++;
                return;
            }
                       
            applyOps(obj);
            _oplogEntryApplies++;       
    }
    
    private:

    void parseOplogMetadataFile(string filePath, map <string, BSONObj> &OplogMetadata) {
        long long fileSize = boost::filesystem::file_size(filePath);
        ifstream file(filePath.c_str(), ios_base::in);

        scoped_ptr<char> buf(new char[fileSize]);
        while (file.getline (buf.get(), fileSize) && !file.eof()) {
          int objSize;
          BSONObj obj;
          obj = fromjson (buf.get(), &objSize); 
          const string seq = obj.getField( "seq" ).valuestr();
          OplogMetadata.insert( pair<string, BSONObj> (seq, obj.getOwned()) );
        }
    }

    bool _usingMongos;
    BSONObj _query;
};

volatile bool Backup::running = false;
namespace proc_mgmt {
    static void fatal_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        warning() << "Dying immediately on fatal signal." << endl;
       
        ::abort();
    }
    static void exit_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        log() << "Will exit soon." << endl;
        Backup::running = false;
    }

}

int main( int argc , char ** argv, char ** envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    Backup backup;
    backup.running = true;
    signal(SIGILL, proc_mgmt::fatal_handler);
    signal(SIGABRT, proc_mgmt::fatal_handler);
    signal(SIGFPE, proc_mgmt::fatal_handler);
    signal(SIGSEGV, proc_mgmt::fatal_handler);
    signal(SIGHUP, proc_mgmt::exit_handler);
    signal(SIGINT, proc_mgmt::exit_handler);
    signal(SIGQUIT, proc_mgmt::exit_handler);
    signal(SIGPIPE, proc_mgmt::exit_handler);
    signal(SIGALRM, proc_mgmt::exit_handler);
    signal(SIGTERM, proc_mgmt::exit_handler);
    signal(SIGUSR1, SIG_IGN);
    signal(SIGUSR2, SIG_IGN);
    return backup.main( argc , argv );
}
