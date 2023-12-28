package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Service
@Slf4j
public class DanmuServicelmpl implements DanmuService {

    @Autowired
    private DataSource dataSource;


    // danmu send --mid 1384516 --pwd "Y*$=o90vEv38^6"--bv "BV1yX4y1T7nY" --content "测试弹幕'" --time 68.80899810791016
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time)
    {
        int id=0;
        long mid = isValidUser(auth);
        if (mid == 0)
            return -1;
        if(!checkBv(bv)||Objects.equals(content, "")
                ||content==null||!checkUnWatched(mid,bv)){
            return -1;
        }

// insert into Danmu(video_bv, sender_mid, send_time, content, post_time) values('BV1yX4y1T7nY',1384516,68.80899810791016,'测试弹幕',now())
        String sql3="select max(id) as id from danmu";
        String sql="insert into danmu ( video_bv, sender_mid, send_time, content, post_time) values(?,?,?,?,now())\n";
        String sql2="select duration from videos where bv= ?;\n";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3);

        ) {

            float duration = 0;
             stmt2.setString(1,bv);
            ResultSet rs = stmt2.executeQuery();
            while(rs.next()){
               duration=rs.getFloat("duration");
            }
            if(duration<time)
            {
                return -1;
            }
           // stmt.setInt(1, id+1 );

            stmt.setString(1, bv);
            stmt.setLong(2,mid);
            stmt.setFloat(3, time);
            stmt.setString(4, content);
            stmt.execute();
            ResultSet rs2 = stmt3.executeQuery();
            while(rs2.next()){
                id=rs2.getInt("id");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }
    //danmu display --bv "BV1yX4y1T7nY" --timeStart  0  --timeEnd 68.80899810791016 --filter false
    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if(!checkBv(bv)||timeEnd<0||timeStart<0||timeEnd<timeStart){
            return null;
        }
        List<Long> Danmu=new ArrayList<>();
        String sql;
        if(filter){
            sql="select distinct id from danmu where video_bv = ? and send_time >= ? and send_time <= ? ;  ";
        }else {
            sql="select  id from danmu where video_bv = ? and send_time >= ? and send_time <= ? ;";
        }
        String sql2="select duration from videos where bv= ?;\n";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
        ) {
            float duration = 0;
            stmt2.setString(1,bv);
            ResultSet rs1 = stmt2.executeQuery();
            while(rs1.next()){
                duration=rs1.getFloat("duration");
            }
            if(duration<timeEnd)
            {
                return null;
            }
            stmt.setString(1,bv);
            stmt.setFloat(2,timeStart);
            stmt.setFloat(3,timeEnd);
         //   System.out.println(sql);
            ResultSet rs = stmt.executeQuery();
            while(rs.next()){
                Danmu.add(rs.getLong("id"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Danmu;
    }
    // danmu like --mid 1384516 --pwd "Y*$=o90vEv38^6" --id 1
    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if(!checkDanmu(id)){
            return false;
        }
        long mid = isValidUser(auth);
        if (mid == 0){
            System.out.println("mid");
            return false;
        }

        String sql2="select * from liked_danmu where danmu_id=? and liker_mid  = ?;";
        String sql="insert into liked_danmu(liker_mid,danmu_id)VALUES (?,?)";
        String sql3="select * from watched where viewer_mid=? and video_bv=(select Danmu.video_bv from danmu where id=?)";
        String sql4="delete from liked_danmu where liker_mid=? and danmu_id=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
              PreparedStatement stmt3 = conn.prepareStatement(sql3);
              PreparedStatement stmt4 = conn.prepareStatement(sql4)
        ) {
            stmt3.setLong(1,mid);
            stmt3.setLong(2,id);
            ResultSet rs3 = stmt3.executeQuery();
            if (!rs3.next()) {
                return false;
            }
            stmt2.setLong(1,id);
            stmt2.setLong(2,mid);
            ResultSet rs = stmt2.executeQuery();
            stmt2.setLong(2,id);
            stmt2.setLong(1,mid);
            stmt4.setLong(1,mid);
            stmt4.setLong(2,id);
            if (rs.next()) {
                stmt4.execute();
                return true;
            }
            stmt.setLong(1,mid);
            stmt.setLong(2,id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }



    public boolean checkBv (String bv){
        String InvalidBv = " select * from videos where bv = ? ";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(InvalidBv)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
             //  System.out.println("checkbv");
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public long isValidUser(AuthInfo auth){
        try{
            String sql = "select * from users where qq = ?";
            Connection con = dataSource.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            if(auth.getQq()!=null){

                ps.setString(1,auth.getQq());;
                ResultSet rs = ps.executeQuery();
                if(rs.next()){
                    long mid = rs.getLong("mid");
                    con.close();
                    return mid;
                }
            }else if(auth.getWechat()!=null){
                ps = con.prepareStatement("select * from users where wechat = ?");
                ps.setString(1,auth.getWechat());
                ResultSet rs = ps.executeQuery();
                if(rs.next()){
                    long mid = rs.getLong("mid");
                    con.close();
                    return mid;
                }
            }else if(auth.getMid()>0&&auth.getPassword()!=null){
                ps = con.prepareStatement("select * from users where mid = ?");
                ps.setLong(1,auth.getMid());
                ResultSet rs = ps.executeQuery();
                if(rs.next()){
                    if(rs.getString("password").equals(auth.getPassword())){
                        long mid = rs.getLong("mid");
                        con.close();
                        return mid;
                    }
                }
            }
            con.close();
            return 0;
        }catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public  boolean checkDanmu ( long id){
        String InvalidDanmu = " select * from Danmu where id = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(InvalidDanmu)) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
    public  boolean checkPostTime (String bv){
        String query = "SELECT * FROM videos WHERE bv = ? AND public_time <= now()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                System.out.println("checkPostTime");
                return false;
            }
            // 如果结果集有下一行数据，说明查询到了结果，返回 true；否则返回 false
            return true;
        } catch (SQLException e) {
            // 捕获异常并进行适当的处理，比如记录日志或者向用户显示错误信息
            throw new RuntimeException("Error executing the query", e);
        }
    }


    public boolean checkUnWatched (long mid,String bv){
        String Unwatch = "select * from watched where viewer_mid=? and video_bv= ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(Unwatch)) {
            stmt.setLong(1, mid);
            stmt.setString(2, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
              //  System.out.println("checkUnWatched");
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }


}
