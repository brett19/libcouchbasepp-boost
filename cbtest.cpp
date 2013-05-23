#include "stdafx.h"
#include "couchbasepp.hpp"

static std::string tme() {
    static int32_t stime = GetTickCount();
    std::string mtme = std::to_string( GetTickCount()-stime );
    while(mtme.size() < 6) {
        mtme = " " + mtme;
    }
    return mtme;
}

int main(void)
{
    // io_service to handle async io
    boost::asio::io_service io_service; 

    // some fake work to keep the io_service running when there is nothing to do
    boost::asio::io_service::work work(io_service); 

    // our couchbase instance
    couchbasepp::instance *cb = new couchbasepp::instance("*******", "*****", "******", "*******", io_service);

    // connect to cb and get some data
    cb->connect([cb](){
        for(int i = 0; i < 1000; ++i ) 
        {
            std::string randkey = "acnt-" + std::to_string(50+i);
            cb->get(randkey, [cb,i](const boost::system::error_code& ec, const std::string &key, const std::string &value, int64_t cas){
                if( i % 10 == 9 ) {
                    if(!ec){
                        std::cout << "[" << tme() << "] " << key << std::endl;
                    } else {
                        std::cerr << "[" << tme() << "] get error: " << ec << std::endl;
                    }
                }
            });
        }
    });

    // run our event loop
    io_service.run();

    // pause to let the user see any messages and then quit
    system("PAUSE");
    return 0;
}