namespace DB.Snapshot
    open System.Threading.Tasks
    open System.Data.SqlClient
    open System
    open System.Data

    type DatabaseSnapshot(dbName: string, dbSnapshotPath: string, dbSnapshotName, conString: string) =
        let executeAsync (sql: string) =
            use con = new SqlConnection(conString)
            async {
                do! con.OpenAsync() |> Async.AwaitTask
                let cmd = new SqlCommand(sql, con)
                cmd.CommandType <- CommandType.Text
                
                let! _ = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
                do con.Close()
            } |> Async.StartAsTask
            
        let execute (sql: string) =
            use con = new SqlConnection(conString)
            con.Open()
            let cmd = new SqlCommand(sql, con)
            cmd.CommandType <- CommandType.Text
                
            cmd.ExecuteNonQuery() |> ignore
            con.Close()
        
        member this.CreateSnapshotAsync (): Task<unit> =
            if not <| System.IO.Directory.Exists(dbSnapshotPath) then
                System.IO.Directory.CreateDirectory(dbSnapshotPath) |> ignore
                
            let query = sprintf "CREATE DATABASE %s ON (NAME = [%s], FILENAME='%s\%s.ss') AS SNAPSHOT OF [%s]" dbSnapshotName dbName dbSnapshotPath dbSnapshotName dbName
            
            async {
                do! executeAsync(query) |> Async.AwaitTask
            } |> Async.StartAsTask
            
        member this.CreateSnapshot (): unit =
            if not <| System.IO.Directory.Exists(dbSnapshotPath) then
                System.IO.Directory.CreateDirectory(dbSnapshotPath) |> ignore
                
            let query = sprintf "CREATE DATABASE %s ON (NAME = [%s], FILENAME='%s\%s.ss') AS SNAPSHOT OF [%s]" dbSnapshotName dbName dbSnapshotPath dbSnapshotName dbName
            
            execute(query)
            
        member this.DeleteSnapshotAsync () =
            let sql = sprintf "DROP DATABASE %s" dbSnapshotName
            async {
                do! executeAsync(sql) |> Async.AwaitTask
            } |> Async.StartAsTask
            
        member this.DeleteSnapshot () =
            let sql = sprintf "DROP DATABASE %s" dbSnapshotName
            
            execute(sql)
            
        member this.RestoreSnapshotAsync () =
            let sql = sprintf "USE master; ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE %s FROM DATABASE_SNAPSHOT = '%s'; ALTER DATABASE %s SET MULTI_USER;" dbName dbName dbSnapshotName dbName
            async {
                do! executeAsync(sql) |> Async.AwaitTask
            } |> Async.StartAsTask 
            
        member this.RestoreSnapshot () =
            let sql = sprintf "USE master; ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE %s FROM DATABASE_SNAPSHOT = '%s'; ALTER DATABASE %s SET MULTI_USER;" dbName dbName dbSnapshotName dbName

            execute(sql)
            
        
