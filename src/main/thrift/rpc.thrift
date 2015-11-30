namespace java joshelser.thrift

service HdfsService {
    string ls(1:string directory);
    string pause(1:i32 millis);

    oneway void ls_oneway(1:string directory);
    oneway void pause_oneway(1:i32 millis);
}
