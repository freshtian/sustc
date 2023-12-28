package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



@Service
@Slf4j
public class RecommenderServicelmpl implements RecommenderService {
    @Autowired
    private  DataSource dataSource;
    //  rec video --bv "BV1yX4y1T7nY"
    @Override
    public List<String> recommendNextVideo(String bv) {
        if (!checkbv(bv)) {
            return null;
        }
        List<String> recommend = new ArrayList<>();
        String sql = "with people as (select viewer_mid from watched where video_bv = ?)\n" +
                "select video_bv, count(*) as similarity\n" +
                "from people\n" +
                "         join watched on people.viewer_mid = watched.viewer_mid\n" +
                "where video_bv <> ?\n" +
                "group by video_bv\n" +
                "order by similarity desc\n" +
                "limit 5;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
             stmt.setString(2, bv); // Add bv as parameter for the second placeholder
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                recommend.add(rs.getString("video_bv"));
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return recommend;
    }
    //   rec general  5 10
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if(pageNum<=0||pageSize<=0){
            return null;
        }
        List<String> recommend = new ArrayList<>();
        String sql="\n" +
                "with danmu_count as\n" +
                "         (SELECT CAST(count(*) AS DECIMAL(10, 5)) / CAST(Videos.watched_counter AS DECIMAL(10, 5)) AS avgdanmu,\n" +
                "                 video_bv,\n" +
                "                 COUNT(*)                                                                          as danmu_count,\n" +
                "                 watched_counter\n" +
                "          FROM danmu\n" +
                "                   join videos on Videos.bv = Danmu.video_bv\n" +
                "          GROUP BY video_bv, watched_counter)\n" +
                "select bv,avgWatch,\n" +
                "       case Videos.watched_counter\n" +
                "           when 0 then 0\n" +
                "           else\n" +
                "                       CAST(liked_counter AS DECIMAL(10, 5)) / CAST(Videos.watched_counter AS DECIMAL(10, 5)) +\n" +
                "                       CAST(coin_counter AS DECIMAL(10, 5)) / CAST(Videos.watched_counter AS DECIMAL(10, 5)) +\n" +
                "                       CAST(collected_counter AS DECIMAL(10, 5)) / CAST(Videos.watched_counter AS DECIMAL(10, 5)) +\n" +
                "                       danmu_count.avgdanmu + avgWatch\n" +
                "           end as Score\n" +
                "FROM Videos\n" +
                "         left join(select video_bv, avg(view_time)/Videos.duration as avgWatch, COUNT(*) as Peoplenum from watched join videos on watched.video_bv=Videos.bv\n" +
                "                                                                                       group by video_bv, Videos.duration) w\n" +
                "                  ON Videos.bv = w.video_bv\n" +
                "         join danmu_count on danmu_count.video_bv = Videos.bv\n" +
                "\n" +
                "ORDER BY Score desc\n" +
                "limit ? OFFSET (?-1)*?;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, pageSize);
            stmt.setInt(2, pageNum);
            stmt.setInt(3, pageSize);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                recommend.add(rs.getString("bv"));
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return recommend;
    }
    // rec user -mid 1384516 --pwd "Y*$=o90vEv38^6"
    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if(pageNum<=0||pageSize<=0){
            return  null;
        }
        List<String> recommend = new ArrayList<>();
        List<Long> friend = new ArrayList<>();
        String sql2 ="select followee_mid as mid  from  (\n" +
                "             select follower_mid from follow where followee_mid=? --auth的粉丝\n" +
                "    ) f1\n" +
                "    join (\n" +
                "             select followee_mid from follow where follower_mid=? --auth的关注\n" +
                " )f2 on f1.follower_mid =f2.followee_mid;";
        String sql= "\n" +
                "with friend as (\n" +
                "select followee_mid as mid  from  (\n" +
                "             select follower_mid from follow where followee_mid=? --auth的粉丝\n" +
                ") f1\n" +
                "    join (\n" +
                "             select followee_mid from follow where follower_mid=? --auth的关注\n" +
                " )f2\n" +
                "        on f1.follower_mid =f2.followee_mid)\n" +
                "\n" +
                "             select video_bv,count(*)\n" +
                "             from watched join friend on viewer_mid=mid\n" +
                "             join videos on Videos.bv=watched.video_bv\n" +
                "             join users on Videos.owner_mid= Users.mid\n" +
                "                   where video_bv not in (\n" +
                "            select video_bv from watched where viewer_mid=?   )\n" +
                " group by video_bv,level,public_time\n" +
                "order by\n" +
                "    count(*) desc,level desc ,public_time desc\n" +
                "limit ? offset (?-1)* ?";
        long mid = isValidUser(auth);
        if (mid == 0){
            return null;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
        ) {
        stmt2.setLong(1,mid);
        stmt2.setLong(2,mid);
            ResultSet rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                friend.add(rs2.getLong("mid"));
            }
            if (friend.isEmpty()){
                return  generalRecommendations( pageSize, pageNum);
            }
            stmt.setLong(1, mid);
            stmt.setLong(2,mid);
            stmt.setLong(3, mid);
            stmt.setInt(4, pageSize);
            stmt.setInt(5, pageNum);
            stmt.setInt(6, pageSize);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                recommend.add(rs.getString("video_bv"));
            }

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return recommend;
    }
    //recommend friends --mid 1384516 --pwd "Y*$=o90vEv38^6"
    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if(pageNum<=0||pageSize<=0){
            return  null;
        }
        List<Long> recommend = new ArrayList<>();
        String sql="\n" +
                "with UnfollowPleple as (select *\n" +
                "                from follow\n" +
                "                                       where follower_mid !=  ?),--auth还没关注的人\\n\" +\n" +
                "                     myfollow as (select followee_mid from follow where follower_mid =  ?)--auth已经关注的人\\n\" +\n" +
                "\n" +
                "                select UnfollowPleple.follower_mid, count(*), (select level from Users where mid = UnfollowPleple.follower_mid) level\n" +
                "                from myfollow\n" +
                "                         join UnfollowPleple on myfollow.followee_mid = UnfollowPleple.followee_mid\n" +
                "                where UnfollowPleple.follower_mid not in (select followee_mid from follow where follower_mid =  ?)\n" +
                "                group by UnfollowPleple.follower_mid\n" +
                "\n" +
                "                order by count(*) DESC, level desc,unfollowPleple.follower_mid\n" +
                "                LIMIT ? OFFSET (? - 1) * ?;";

        long mid = isValidUser(auth);
        if (mid == 0)
            return null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1,mid);
            stmt.setLong(2, mid);
            stmt.setLong(3, mid);
            stmt.setInt(4, pageSize);
            stmt.setInt(5, pageNum);
            stmt.setInt(6, pageSize);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                recommend.add(rs.getLong("follower_mid"));
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return recommend;
    }



    public boolean checkbv (String bv){
        String InvalidBv = " select * from videos where bv = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(InvalidBv)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            if (rs.wasNull()) {
                stmt.close();
                conn.close();
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public long isValidUser(AuthInfo auth) {
        try {
            String sql = "select * from users where qq = ?";
            Connection con = dataSource.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            if (auth.getQq() != null) {

                ps.setString(1, auth.getQq());
                ;
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    long mid = rs.getLong("mid");
                    con.close();
                    return mid;
                }
            } else if (auth.getWechat() != null) {
                ps = con.prepareStatement("select * from users where wechat = ?");
                ps.setString(1, auth.getWechat());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    long mid = rs.getLong("mid");
                    con.close();
                    return mid;
                }
            } else if (auth.getMid() > 0 && auth.getPassword() != null) {
                ps = con.prepareStatement("select * from users where mid = ?");
                ps.setLong(1, auth.getMid());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    if (rs.getString("password").equals(DatabaseServiceImpl.hashPassword(auth.getPassword()))) {
                        long mid = rs.getLong("mid");
                        con.close();
                        return mid;
                    }
                }
            }
            con.close();
            return 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
