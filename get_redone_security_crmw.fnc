create or replace function get_redone_security_crmw(in_type          in varchar2, --0:某个CRMW的全部流水, 1:一段时间内，CRMW需要重算提醒的字段流水
                                               in_start_time    in char /*varchar2*/,
                                               in_end_time      in char /*varchar2*/,
                                               in_security_code in SECURITY_DETAIL.SECURITY_CODE%type,
                                               in_mail          in int default 0)
  return out_row_r is

  lc_ret                 out_row_r := out_row_r();
  v_table                varchar2(30);
  v_detail_table         varchar2(30);
  lc_int                 int;
  lc_datastr             varchar2(4000);
  lc_datastr_redone      varchar2(4000);
  v_colname              varchar2(3000);
  v_sel_colname          varchar2(3000);
  v_redone_colname       varchar2(3000);
  v_re_colname           varchar2(3000);
  v_cn_colname           varchar2(3000);
  v_cn_redone_name       varchar2(3000);
  v_redone_col           int;
  V_SQL_STATMENT         varchar2(4000);
  v_check_seq_now        int;
  v_seq_now              int;
  v_seq_min              int;
  v_seq_before           int;
  v_seq_compare          int;
  v_seq                  int;
  v_last_seq             int;
  v_last_result          varchar2(5000);
  v_result               varchar2(5000);
  v_result_str           varchar2(5000);
  v_result_str_2         varchar2(5000);
  v_find_diff            int;
  v_find_need            int;
  v_modify_date          varchar2(50);
  v_check_date           varchar2(50);
  v_check_user           varchar2(50);
  v_modify_user          varchar2(50);
  lc_sep_col             char(1) := '|';
  v_security_name        varchar2(300);
  v_count                char(3);
  v_now_count            char(3);
  v_last_count           char(3);
  ll_b_list              msg_list := msg_list();
  ll_a_list              msg_list := msg_list();
  ll_b_list_detail       msg_list := msg_list();
  ll_a_list_detail       msg_list := msg_list();
  ll_col_list            msg_list := msg_list();
  ll_cn_col_list         msg_list := msg_list();
  ll_is_redone_list      msg_list := msg_list();
  ll_priority_list       msg_list := msg_list();
  v_result_b_list        varchar2(500);
  v_result_a_list        varchar2(500);
  v_result_b_list_detail varchar2(500);
  v_result_a_list_detail varchar2(500);
  v_result_col_list      varchar2(100);
  v_cn_result_col_list   varchar2(100);
  v_cn_redone_list       varchar2(100);
  v_is_redone_list       varchar2(100);
  v_is_redone_list_cn    varchar2(100);
  v_priority_list        varchar2(100);
  v_is_redone            varchar2(500);
  v_is_redone_str        varchar2(500);
  v_priority             varchar2(500);
  v_priority_str         varchar2(500);
  v_payment_date         varchar2(8);
  v_date                 SECURITY_DETAIL.MODIFY_DATE%type;
  v_start_time           date;
  v_end_time             date;
  v_security_code        security.security_code%type;
  is_redone_pri_save     int;
  is_redone_pri          int;

  cursor security_cur is
    select distinct t.security_code
      from security_crmw_check_info t
     where modify_time between v_start_time and v_end_time
       and nvl(trim(t.security_code), ' ') =
           nvl(trim(in_security_code), nvl(trim(t.security_code), ' '))
       and not exists
     (select null
              from security_crmw_check_info st
             where st.table_name = t.table_name
               and st.security_code = t.security_code
               and to_char(st.modify_time, 'yyyymmddHH24MISS') >
                   to_char(t.modify_time, 'yyyymmddHH24MISS'));

  cursor detail_cur(v_tb_sc varchar2) is
    select t.table_name, t.check_seq,t.modify_time
      from security_crmw_check_info t
     where t.security_code = v_tb_sc
     order by t.modify_time;

  cursor security_rule_col_cur(v_tb_name varchar2) is
    select * from SECURITY_CRMW_REDONE_RULE where tb_name = v_tb_name;

  cursor security_redone_rule_cur(v_tb_name varchar2) is
    select *
      from SECURITY_CRMW_REDONE_RULE
     where tb_name = v_tb_name
       and is_redone in ('Y', 'N')
     order by is_redone;

  function get_colname_value(in_sel_colname   IN VARCHAR2,
                             in_table_name    IN VARCHAR2,
                             in_security_code IN VARCHAR2,
                             in_seq           IN NUMBER,
                             in_condition     IN VARCHAR2 default '')
    return varchar2 as
    V_SQL_STATMENT varchar2(4000);
    v_sql_result   varchar2(4000);
  begin
    V_SQL_STATMENT := 'select ' || in_sel_colname || ' from ' ||
                      in_table_name || ' where security_code=''' ||
                      in_security_code || ''' and check_seq=' || in_seq ||
                      in_condition;

    begin
      EXECUTE IMMEDIATE V_SQL_STATMENT
        into v_sql_result;
      v_sql_result := v_sql_result;
    exception
      when others then
        v_sql_result := NULL;
    end;
    return trim(v_sql_result);
  end;
begin
  v_table     := null;
  lc_int      := 0;
  lc_datastr  := null;
  v_find_diff := 0;
  
  if trim(in_start_time) is null then
     v_start_time := to_date('20000101','yyyymmdd');
  else 
     v_start_time := to_date(in_start_time,'yyyymmdd');
  end if;

  if trim(in_end_time) is null then
     v_end_time := to_date('20991231235959','yyyymmddhh24miss');
  else 
     v_end_time := to_date(in_end_time||'235959','yyyymmddhh24miss');
  end if;

  --按CRMW查询
  if (in_type = '0') then
    for security_rec in security_cur loop
	  v_security_code := rpad(security_rec.security_code,16,' ');
      begin
        select t.security_short_name
          into v_security_name
          from security_crmw t
         where t.security_code = v_security_code;

      exception
        when others then
          v_security_name := null;
      end;

      v_security_name := trim(v_security_name);

      if (v_security_name is null) then

        dbms_output.put_line('security_code:[' || security_rec.security_code || ']');
        begin
        select trim(a.security_name)
          into v_security_name
          from security_crmw_detail a
         where a.security_code = v_security_code
           and not exists (select null
                  from security_crmw_detail b
                 where b.security_code = a.security_code
                   and b.check_seq > a.check_seq);
        exception
        when others then
          v_security_name := null;
        end;
        dbms_output.put_line('v_security_name:[' || v_security_name || ']');
        lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                      ' SECURITY_CODE=' || '"' ||
                      security_rec.security_code || '"' ||
                      ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                      ' COLNAME="" VALUE="这只券已被删除" BEFORE_VALUE=""' ||
                      '  USER=' || '"' || v_modify_user || '"' || ' DATE=' || '"' ||
                      v_modify_date || '" />';
        lc_ret.extend;
        lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
        lc_int := lc_int + 1;
        continue;
      end if;

      for detail_rec in detail_cur(trim(security_rec.security_code)) loop

        v_detail_table  := detail_rec.table_name;
        v_last_seq      := detail_rec.check_seq;
        v_check_seq_now := detail_rec.check_seq;
        v_table         := substr(detail_rec.table_name,
                                  1,
                                  length(detail_rec.table_name) - 7);
        dbms_output.put_line(in_type || '--' || v_detail_table || '--' ||
                             v_check_seq_now);
        if(v_last_seq <1) then
          continue;
        end if;
        --CRMW基础信息
        if (v_detail_table = 'SECURITY_CRMW_DETAIL') then
          v_redone_col := 0;
          v_colname    := null;
          v_re_colname := null;
          for security_rule_col_rec in security_rule_col_cur(v_table) loop
            v_colname    := v_colname || '||''|''||' ||
                            security_rule_col_rec.col_name;
            v_re_colname := v_re_colname || lc_sep_col ||
                            -- return col_cn_name to client change by jessica zt20200605
                            --security_rule_col_rec.col_name;
                            security_rule_col_rec.col_cn_name;
            v_redone_col := v_redone_col + 1;
          end loop;
          v_sel_colname    := substr(v_colname, 8) || '||''|''';
          v_redone_colname := substr(v_re_colname, 2) || lc_sep_col;
          v_last_result    := null;
          v_result         := null;
          if (v_sel_colname is not null) then
            <<search_start>>
            loop
              if (v_result is not null) then
                v_last_result := v_result;
              else

                v_last_result := get_colname_value(v_sel_colname,
                                                   v_detail_table,
                                                   security_rec.security_code,
                                                   v_check_seq_now);
              end if;

              --遍历历史的所有字段的数据
              if (v_last_seq > 0 and v_last_result is not null) then

                <<search_loop_all>>
                loop

                  V_SQL_STATMENT := 'select to_char(modify_date,''yyyymmdd HH24:MI:SS'') from ' ||
                                    v_detail_table ||
                                    ' where security_code=''' ||
                                    security_rec.security_code ||
                                    ''' and check_seq=' || v_last_seq;

                  EXECUTE IMMEDIATE V_SQL_STATMENT
                    into v_modify_date;

                  V_SQL_STATMENT := 'select trim(w.user_name)||''(''|| w.user_number||'')'' from ' ||
                                    v_detail_table ||
                                    ' t,wuser w where t.security_code=''' ||
                                    security_rec.security_code ||
                                    ''' and t.check_seq=' || v_last_seq ||
                                    ' and t.modify_user=w.user_number';

                  begin
                    EXECUTE IMMEDIATE V_SQL_STATMENT
                      into v_modify_user;
                  exception
                    when others then
                      V_SQL_STATMENT := 'select ''(''|| t.modify_user||'')'' from ' ||
                                        v_detail_table ||
                                        ' t where t.security_code=''' ||
                                        security_rec.security_code ||
                                        ''' and t.check_seq=' || v_last_seq;
                      EXECUTE IMMEDIATE V_SQL_STATMENT
                        into v_modify_user;
                  end;
                  v_last_seq  := v_last_seq - 1;
                  v_result    := get_colname_value(v_sel_colname,
                                                   v_detail_table,
                                                   security_rec.security_code,
                                                   v_last_seq);
                
                  v_find_diff := 0;
                  if (v_result <> v_last_result) then
                    v_find_diff := 1;
                    dbms_output.put_line('v_result: is diff!');
                  end if;
                  v_check_seq_now := v_last_seq;
                  exit search_loop_all when(v_last_seq = 0 or
                                            v_find_diff = 1);
                end loop;
              end if;
              --记录有变动，查找变动的字段
              if (v_find_diff = 1) then

                ll_a_list   := string_split(v_last_result, lc_sep_col);
                ll_b_list   := string_split(v_result, lc_sep_col);
                ll_col_list := string_split(v_redone_colname, lc_sep_col);
                for i in 1 .. v_redone_col loop
                  v_result_a_list   := trim(ll_a_list(i).message);
                  v_result_b_list   := trim(ll_b_list(i).message);
                  v_result_col_list := ll_col_list(i).message;
                  if (nvl(v_result_a_list, lc_sep_col) <> nvl(v_result_b_list, lc_sep_col)) then
                    dbms_output.put_line(v_result_col_list ||
                                         '列信息变更！现在是:[' || v_result_a_list ||
                                         '],原来是:[' || v_result_b_list || ']');
                                                             
                    lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                                  ' SECURITY_CODE=' || '"' ||
                                  security_rec.security_code || '"' ||
                                  ' SECURITY_NAME=' || '"' ||
                                  v_security_name || '"' || ' COLNAME=' || '"' ||
                                  v_result_col_list || '"' || ' VALUE=' || '"' ||
                                  v_result_a_list || '"' ||
                                  ' BEFORE_VALUE=' || '"' ||
                                  v_result_b_list || '"' || '  USER=' || '"' ||
                                  v_modify_user || '"' || ' DATE=' || '"' ||
                                  v_modify_date || '" />';
                    lc_ret.extend;
                    lc_ret(lc_ret.last) := out_row_entry_r(lc_int,
                                                           lc_datastr);
                    lc_int := lc_int + 1;
                  end if;
                end loop;
              end if;
              exit search_start when(v_check_seq_now = 0);
            end loop;
          end if;
        end if;
        --有好几期数据的非CRMW基础信息表
        if (v_detail_table in
           ('CRMW_PYMN_SCHD_DETAIL', 'CRMW_COUPON_SCHD_DETAIL','SECURITY_CREDIT_EVENTS_DETAIL')) then
          v_colname    := null;
          v_re_colname := null;
          v_redone_col := 0;

		  for security_rule_col_rec in security_rule_col_cur(v_table) loop
            v_colname    := v_colname || '||''|''||' ||
                            security_rule_col_rec.col_name;
            v_re_colname := v_re_colname || lc_sep_col ||
            -- return col_cn_name to client change by jessica zt20200605
                            security_rule_col_rec.col_cn_name;
            v_redone_col := v_redone_col + 1;
          end loop;

          v_sel_colname    := substr(v_colname, 8) || '||''|''';
          v_redone_colname := substr(v_re_colname, 2) || lc_sep_col;

          --循环查询
          if (v_sel_colname is not null) then
            <<search_details_start>>
            loop
              --查询当前的期数
              V_SQL_STATMENT := 'select max(seq), min(seq) from ' || v_detail_table ||
                                ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and check_seq=' || v_check_seq_now;

              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_seq_now,v_seq_min ;
              exception
                when others then
                  v_seq_now := -1;
              end;

              if (v_seq_now is null) then
                v_check_seq_now := v_check_seq_now - 1;
                exit search_details_start when(v_check_seq_now = 0);
                continue;
              end if;

              --查询当期的修改时间、修改人
              v_last_seq     := v_check_seq_now;
              V_SQL_STATMENT := 'select to_char(modify_date,''yyyymmdd HH24:MI:SS'') from ' ||
                                v_detail_table || ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and check_seq=' || v_last_seq ||
                                ' and seq='||v_seq_min;

              EXECUTE IMMEDIATE V_SQL_STATMENT
                into v_modify_date;

              V_SQL_STATMENT := 'select trim(w.user_name)||''(''|| w.user_number||'')'' from ' ||
                                v_detail_table ||
                                ' t,wuser w where t.security_code=''' ||
                                security_rec.security_code ||
                                ''' and t.check_seq=' || v_last_seq ||
                                ' and t.seq='||v_seq_min ||' and t.modify_user=w.user_number';

              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_modify_user;
              exception
                when others then
                  V_SQL_STATMENT := 'select ''(''|| t.modify_user||'')'' from ' ||
                                    v_detail_table ||
                                    ' t where t.security_code=''' ||
                                    security_rec.security_code ||
                                    ''' and t.check_seq=' || v_last_seq ||
                                    ' and t.seq='||v_seq_min;
                  EXECUTE IMMEDIATE V_SQL_STATMENT
                    into v_modify_user;
              end;

              --上一次的期数
              v_last_seq := v_last_seq - 1;

              V_SQL_STATMENT := 'select max(seq) from ' || v_detail_table ||
                                ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and check_seq=' || v_last_seq;

              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_seq_before;
              exception
                when others then
                  v_seq_before := -1;
              end;

              if (v_seq_before is not null and v_seq_before > -1) then
                v_seq_compare := v_seq_now;
                if (v_seq_before < v_seq_now) then
                  v_seq_compare := v_seq_before;
                end if;
              end if;

              v_seq := v_seq_min;
              <<search_before_seq_loop>>
              loop
                v_result_str := get_colname_value(v_sel_colname,
                                                  v_detail_table,
                                                  security_rec.security_code,
                                                  v_check_seq_now,
                                                  ' and seq=' || v_seq);

                v_result_str_2 := get_colname_value(v_sel_colname,
                                                    v_detail_table,
                                                    security_rec.security_code,
                                                    v_last_seq,
                                                    ' and seq=' || v_seq);

                --- 增加判断期数不连续的情况
                if (nvl(v_result_str, lc_sep_col) <> nvl(v_result_str_2, lc_sep_col)) and (v_result_str is not null and v_result_str_2 is not null) then
                  v_count := lpad(v_seq, 3, '0');
                  dbms_output.put_line(v_count || '期还本付息信息变更！现在是:[' ||
                                       v_result_str || '],原来是:[' ||
                                       v_result_str_2 || ']');

                  ll_a_list_detail := string_split(v_result_str ||
                                                   lc_sep_col,
                                                   lc_sep_col);
                  ll_b_list_detail := string_split(v_result_str_2 ||
                                                   lc_sep_col,
                                                   lc_sep_col);
                  ll_col_list      := string_split(v_redone_colname,
                                                   lc_sep_col);

                  v_re_colname   := null;
                  v_result_str   := null;
                  v_result_str_2 := null;
                  for j in 1 .. v_redone_col loop
                    v_result_a_list_detail := trim(ll_a_list_detail(j)
                                                   .message);
                    v_result_b_list_detail := trim(ll_b_list_detail(j)
                                                   .message);
                    v_result_col_list      := ll_col_list(j).message;

                    if (nvl(v_result_a_list_detail, lc_sep_col) =
                       nvl(v_result_b_list_detail, lc_sep_col)) then
                      continue;
                    end if;
                    v_re_colname := v_re_colname || lc_sep_col ||
                                    v_result_col_list;

                    v_result_str   := v_result_str || lc_sep_col ||
                                      v_result_a_list_detail;
                    v_result_str_2 := v_result_str_2 || lc_sep_col ||
                                      v_result_b_list_detail;

                  end loop;
                  v_result_a_list_detail := substr(v_result_str, 2);
                  v_result_b_list_detail := substr(v_result_str_2, 2);
                  v_result_col_list      := substr(v_re_colname, 2);

                  dbms_output.put_line(v_count || '期' || v_result_col_list ||
                                       '字段变更！现在是:[' ||
                                       v_result_a_list_detail || '],原来是:[' ||
                                       v_result_b_list_detail || ']');

                  lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                                ' SECURITY_CODE=' || '"' ||
                                security_rec.security_code || '"' ||
                                ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                                ' COLNAME=' || '"' || v_count || '期:' ||
                                v_result_col_list || '"' || ' VALUE=' || '"' ||
                                v_result_a_list_detail || '"' ||
                                ' BEFORE_VALUE=' || '"' ||
                                v_result_b_list_detail || '"' || '  USER=' || '"' ||
                                v_modify_user || '"' || ' DATE=' || '"' ||
                                v_modify_date || '" />';
                  lc_ret.extend;
                  lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
                  lc_int := lc_int + 1;

                  --exit;
                end if;

                v_seq := v_seq + 1;
                exit search_before_seq_loop when(v_seq > v_seq_compare);
              end loop;

              if (v_seq_before <> v_seq_now) then
                v_now_count  := lpad(v_seq_now, 3, '0');
                v_last_count := lpad(v_seq_before, 3, '0');
                lc_datastr   := '<DATA TYPE=' || '"' || in_type || '"' ||
                                ' SECURITY_CODE=' || '"' ||
                                security_rec.security_code || '"' ||
                                ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                                ' COLNAME="期数变动" VALUE=' || '"' ||
                                v_now_count || '期"' || ' BEFORE_VALUE=' || '"' ||
                                v_last_count || '期"' || '  USER=' || '"' ||
                                v_modify_user || '"' || ' DATE=' || '"' ||
                                v_modify_date || '" />';
                lc_ret.extend;
                lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
                lc_int := lc_int + 1;
              end if;

              v_check_seq_now := v_last_seq;
              exit search_details_start when(v_check_seq_now = 0);
            end loop;
          end if;

        end if;

      end loop;

    end loop;
    
    --按日期查询
  elsif (in_type = '1') then
    for security_rec in security_cur loop
	  v_check_date := null; --- 初始化 v_check_date
	  v_security_code := rpad(security_rec.security_code,16,' ');
      begin
        select t.security_short_name
          into v_security_name
          from security_crmw t
         where t.security_code = v_security_code;

      exception
        when others then
          v_security_name := null;
      end;

      if (v_security_name is null) then
        continue;
      end if;

      v_security_name := trim(v_security_name);
      v_find_need     := 0;
      for detail_rec in detail_cur(trim(security_rec.security_code)) loop

        v_detail_table  := detail_rec.table_name;
        v_table         := substr(detail_rec.table_name,
                                  1,
                                  length(detail_rec.table_name) - 7);
        v_last_seq      := detail_rec.check_seq;
        v_check_seq_now := detail_rec.check_seq;
        if (v_last_seq = 0) then
          continue;
        end if;

        --审核人/审核时间
        V_SQL_STATMENT := 'select nvl(to_char(max(modify_date),''yyyymmddhh24miss''),'' '') from security_modify_log where security_code=''' ||
                          security_rec.security_code ||
                          ''' and note=''gen sync crmw file'' ';

        EXECUTE IMMEDIATE V_SQL_STATMENT
          into v_check_date;

        v_check_user := null;
        if (trim(v_check_date) is not null) then
          V_SQL_STATMENT := 'select trim(w.user_name) || ''('' || a.Modify_User || '')'' from security_modify_log a,wuser w where to_char(a.modify_date,''yyyymmddhh24miss'')=' ||
                            to_char(v_check_date) || '
                               and w.user_number= a.modify_user and a.security_code=''' ||
                            security_rec.security_code ||
                            ''' and rownum=1 and a.note=''gen sync crmw file'' ';
         
          begin
            EXECUTE IMMEDIATE V_SQL_STATMENT
              into v_check_user;
          exception
            when others then
              v_check_user := 'system';
          end;

          V_SQL_STATMENT := 'select to_char(to_date(' || v_check_date ||
                            ',''yyyy-mm-dd hh24:mi:ss''),''yyyymmdd hh24:mi:ss'') from dual';

          EXECUTE IMMEDIATE V_SQL_STATMENT
            into v_check_date;
        end if;

        v_colname    := null;
        v_re_colname := null;
        v_redone_col := 0;
        v_is_redone  := null;
        v_cn_colname := null;
        for security_redone_rule_rec in security_redone_rule_cur(v_table) loop
         v_colname    := v_colname || '||''|''||' ||
                          security_redone_rule_rec.col_name;
          v_re_colname := v_re_colname || lc_sep_col ||
                          security_redone_rule_rec.col_name;
          v_cn_colname := v_cn_colname || lc_sep_col ||
                          security_redone_rule_rec.col_cn_name;
          v_redone_col := v_redone_col + 1;
          v_is_redone  := v_is_redone || lc_sep_col ||
                          security_redone_rule_rec.is_redone;
          v_priority  := v_priority || lc_sep_col ||
                          security_redone_rule_rec.priority;
						  
        end loop;
        v_sel_colname    := substr(v_colname, 8) || '||''|''';
        v_redone_colname := substr(v_re_colname, 2) || lc_sep_col;
        v_cn_redone_name := substr(v_cn_colname, 2) || lc_sep_col;
        v_is_redone_str  := substr(v_is_redone, 2) || lc_sep_col;
		v_priority_str  := substr(v_priority, 2) || lc_sep_col;
        if (v_detail_table = 'SECURITY_CRMW_DETAIL' and v_find_need = 0) then
          v_last_result := get_colname_value(v_sel_colname,
                                             v_detail_table,
                                             security_rec.security_code,
                                             v_last_seq);

          --3.遍历历史数据的重算字段
          if (v_last_seq > 0 and v_last_result is not null) then
            begin
            V_SQL_STATMENT := 'select modify_date from ' || v_detail_table ||
                              ' where security_code=''' ||
                              security_rec.security_code ||
                              ''' and check_seq= 0';

            EXECUTE IMMEDIATE V_SQL_STATMENT

              into v_date;
            exception
			  when others then
			  v_date := null;
            end; 
            /*
            CRMW的修改判断: 新券check_seq=0的modify_date是审核当天
            */

            --新CRMW数据，则与审核当天的首笔数据进行比对
            if (v_check_date is not null and v_date is not null and to_char(v_date, 'yyyymmdd') = substr(v_check_date,0,8)) then
              v_result    := get_colname_value(v_sel_colname,
                                               v_detail_table,
                                               security_rec.security_code,
                                               0);
              v_find_diff := 0;
              if (v_result <> v_last_result and v_result is not null) then
                v_find_diff := 1;
              end if;

              --已有CRMW，则与当日之前日期的数据比对
            else
              V_SQL_STATMENT := 'select max(check_seq) from ' ||
                                v_detail_table || ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and to_char(modify_date,''yyyymmdd'') < substr(''' ||
                                v_check_date || ''',0,8) and check_seq <' ||
                                v_last_seq;
              /*dbms_output.put_line('V_SQL_STATMENT:'||V_SQL_STATMENT);*/
              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_last_seq;
              exception
                when others then
                  v_last_seq := 0;
              end;
              if (v_last_seq is null) then
                v_last_seq := 0;
              end if;
              v_result    := get_colname_value(v_sel_colname,
                                               v_detail_table,
                                               security_rec.security_code,
                                               v_last_seq);
              v_find_diff := 0;
              if (v_result <> v_last_result and v_result is not null) then
                v_find_diff := 1;
              end if;
            end if;
          end if;
		  is_redone_pri_save := null;
          --4.重算字段有变动，找出字段和值
          if (v_find_diff = 1) then

            ll_a_list         := string_split(v_last_result, lc_sep_col);
            ll_b_list         := string_split(v_result, lc_sep_col);
            ll_col_list       := string_split(v_redone_colname, lc_sep_col);
            ll_cn_col_list    := string_split(v_cn_redone_name, lc_sep_col);
            ll_is_redone_list := string_split(v_is_redone_str, lc_sep_col);
			ll_priority_list := string_split(v_priority_str, lc_sep_col);
            for i in 1 .. v_redone_col loop
              v_result_a_list := trim(ll_a_list(i).message);
              v_result_b_list := trim(ll_b_list(i).message);
              if (nvl(v_result_a_list, lc_sep_col) <> nvl(v_result_b_list, lc_sep_col)) then

                v_result_col_list := ll_col_list(i).message;
                v_is_redone_list  := ll_is_redone_list(i).message;
				v_priority_list  := ll_priority_list(i).message;
                v_cn_redone_list  := ll_cn_col_list(i).message;
                
                /*dbms_output.put_line(v_result_col_list || '列信息变更！现在是:[' ||
                v_result_a_list || '],原来是:[' ||
                v_result_b_list || ']');*/

                if (trim(v_is_redone_list) = 'Y') then
                  v_is_redone_list_cn := '重算字段';
                else
                  v_is_redone_list_cn := '通知字段';
                end if;
                if (in_mail = 1) then
                  lc_datastr := '凭证代码:' || security_rec.security_code ||
                                ' 凭证简称:' || v_security_name || ' 类型:' ||
                                v_is_redone_list_cn || ' 修改字段:' ||
                                nvl(trim(v_cn_redone_list),
                                    v_result_col_list) || ' 现数据:' ||
                                v_result_a_list || ' 原数据:' ||
                                v_result_b_list || '  审核人:' || v_check_user ||
                                ' 审核通过时间:' || v_check_date;
                else
                  lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                                ' SECURITY_CODE=' || '"' ||
                                security_rec.security_code || '"' ||
                                ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                                ' IS_REDONE=' || '"' || v_is_redone_list || '"' ||
                                -- return col_cn_name to client change by jessica at 20200605
                                --' COLNAME=' || '"' || v_result_col_list || '"' ||
                                ' COLNAME=' || '"' || v_cn_redone_list || '"' ||
                                ' VALUE=' || '"' || v_result_a_list || '"' ||
                                ' BEFORE_VALUE=' || '"' || v_result_b_list || '"' ||
                                '  USER=' || '"' || v_check_user || '"' ||
                                ' DATE=' || '"' || v_check_date || '" />';
                end if;
				-- 重算字段需按优先级决定展示
                --lc_ret.extend;
                --lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
                --lc_int := lc_int + 1;
                if (trim(v_is_redone_list) = 'Y') then
                  --v_find_need := 1;
				  if nvl(is_redone_pri_save,99) <  to_number(nvl(trim(v_priority_list),'99')) then
				     lc_datastr_redone := lc_datastr;
				     is_redone_pri_save := to_number(nvl(trim(v_priority_list),'99'));
				  end if;
                  --exit;
				else
				  lc_ret.extend;
                  lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
                  lc_int := lc_int + 1;
                end if;
              end if;
            end loop;
          end if;
		  if nvl(is_redone_pri_save,99) = 1 then
		     v_find_need := 1;
			 lc_ret.extend;
             lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
             lc_int := lc_int + 1;				 
          end if;		  
        elsif (v_detail_table in
              ('CRMW_PYMN_SCHD_DETAIL', 'CRMW_COUPON_SCHD_DETAIL','SECURITY_CREDIT_EVENTS_DETAIL') and
              v_find_need = 0) then
          --2.1 查询当前的期数
          V_SQL_STATMENT := 'select max(seq),min(seq) from ' || v_detail_table ||
                            ' where security_code=''' ||
                            security_rec.security_code ||
                            ''' and check_seq=' || v_check_seq_now;

          begin
            EXECUTE IMMEDIATE V_SQL_STATMENT
              into v_seq_now,v_seq_min;
          exception
            when others then
              v_seq_now := -1;
			  v_seq_min := -1;
          end;

          --2.2 当前期数
          if (v_seq_now is not null and v_seq_now > -1) then

            /*
             CRMW的比较逻辑：
             1.新CRMW与审核当天的首笔数据进行比对
             2.已有券与上一次数据进行比对
            */
						
            --首笔数据时间
            V_SQL_STATMENT := 'select modify_date from ' || v_detail_table ||
                              ' where security_code=''' ||
                              security_rec.security_code ||
                              ''' and check_seq=0 and seq='||v_seq_min;
            begin
            EXECUTE IMMEDIATE V_SQL_STATMENT
              into v_date;
			exception
			  when others then
			  v_date := null;
            end;
			
            -- 新券
            if (v_check_date is not null and v_date is not null and to_char(v_date, 'yyyymmdd') = substr(v_check_date,0,8)) then

              V_SQL_STATMENT := 'select max(seq) from ' || v_detail_table ||
                                ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and check_seq = 0';

              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_seq_before;
              exception
                when others then
                  v_seq_before := -1;
              end;
              v_last_seq := 0;
            else

              V_SQL_STATMENT := 'select max(check_seq) from ' ||
                                v_detail_table ||
                                ' s where security_code=''' ||
                                security_rec.security_code ||
                                ''' and to_char(modify_date,''yyyymmdd'') < substr(''' ||
                                v_check_date ||
                                ''',0,8) and seq='||v_seq_min || ' and check_seq <' ||
                                v_check_seq_now;
              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_last_seq;
              exception
                when others then
                  v_last_seq := 0;
              end;
              if (v_last_seq is null) then
                v_last_seq := 0;
              end if;

              V_SQL_STATMENT := 'select max(seq) from ' || v_detail_table ||
                                ' where security_code=''' ||
                                security_rec.security_code ||
                                ''' and check_seq = ' || v_last_seq;

              begin
                EXECUTE IMMEDIATE V_SQL_STATMENT
                  into v_seq_before;
              exception
                when others then
                  v_seq_before := -1;
              end;

            end if;

            --需要比较的期数
            v_seq_compare := v_seq_now;

            if (v_seq_before < v_seq_now) then
              v_seq_compare := v_seq_before;
            end if;

            --循环比较每一期
			
			v_seq := v_seq_min - 1;
			
            <<search_before_seq_loop>>
            loop

              v_seq := v_seq + 1;
              exit search_before_seq_loop when(v_seq > v_seq_compare or v_find_need = 1);

              v_result_str := get_colname_value(v_sel_colname,
                                                v_detail_table,
                                                security_rec.security_code,
                                                v_check_seq_now,
                                                ' and seq=' || v_seq);

              v_result_str_2 := get_colname_value(v_sel_colname,
                                                  v_detail_table,
                                                  security_rec.security_code,
                                                  v_last_seq,
                                                  ' and seq=' || v_seq);

              --记录不一样的那期数据，找出有变动的字段
              --- 增加判断期数不连续的情况		  
              if (nvl(v_result_str, lc_sep_col) <> nvl(v_result_str_2, lc_sep_col)) and (v_result_str is not null and v_result_str_2 is not null) then			  
                v_count := lpad(v_seq, 3, '0');

                ll_a_list_detail := string_split(v_result_str || lc_sep_col,
                                                 lc_sep_col);
                ll_b_list_detail := string_split(v_result_str_2 ||
                                                 lc_sep_col,
                                                 lc_sep_col);
                ll_col_list      := string_split(v_redone_colname,
                                                 lc_sep_col);
                ll_cn_col_list   := string_split(v_cn_redone_name,
                                                 lc_sep_col);
                ll_is_redone_list := string_split(v_is_redone_str, lc_sep_col);												 
				ll_priority_list := string_split(v_priority_str, lc_sep_col);	
				
                v_re_colname     := null;
                v_cn_colname     := null;
                v_result_str     := null;
                v_result_str_2   := null;
				is_redone_pri := null;
                for j in 1 .. v_redone_col loop
                  v_result_a_list_detail := trim(ll_a_list_detail(j).message);
                  v_result_b_list_detail := trim(ll_b_list_detail(j).message);
                  v_result_col_list      := ll_col_list(j).message;
                  v_cn_redone_list       := ll_cn_col_list(j).message;
				  v_is_redone_list       := ll_is_redone_list(j).message;				  
				  v_priority_list       := ll_priority_list(j).message;

                  if (nvl(v_result_a_list_detail, lc_sep_col) =
                     nvl(v_result_b_list_detail, lc_sep_col)) then
                    continue;
                  end if;

                  v_re_colname   := v_re_colname || lc_sep_col ||
                                    v_result_col_list;
                  v_cn_colname   := v_cn_colname || lc_sep_col ||
                                    v_cn_redone_list;
                  v_result_str   := v_result_str || lc_sep_col ||
                                    v_result_a_list_detail;
                  v_result_str_2 := v_result_str_2 || lc_sep_col ||
                                    v_result_b_list_detail;
				  if  v_priority_list is not null and v_is_redone_list='Y'  and nvl(is_redone_pri,99) > to_number(trim(v_priority_list)) then
				         is_redone_pri := to_number(trim(v_priority_list));
                  end if;
                end loop;
                v_result_a_list_detail := substr(v_result_str, 2);
                v_result_b_list_detail := substr(v_result_str_2, 2);
                v_result_col_list      := substr(v_re_colname, 2);
                v_cn_result_col_list   := substr(v_cn_colname, 2);

                /* dbms_output.put_line(v_count || '期' || v_result_col_list ||
                '字段变更！现在是:[' ||
                v_result_a_list_detail || '],原来是:[' ||
                v_result_b_list_detail || ']');*/


                if (in_mail = 1) then
                  lc_datastr := '凭证代码:' || security_rec.security_code ||
                                ' 凭证简称:' || v_security_name ||
                                ' 类型:重算字段 修改字段:(' || v_count || '期:' ||
                                v_cn_result_col_list || ') 现数据:' ||
                                v_result_a_list_detail || ' 原数据:' ||
                                v_result_b_list_detail || '  审核人:' ||
                                v_check_user || ' 审核通过时间:' || v_check_date;
                else
                  lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                                ' SECURITY_CODE=' || '"' ||
                                security_rec.security_code || '"' ||
                                ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                                ' IS_REDONE=' || '"Y"' || ' COLNAME=' || '"' ||
                                -- return col_cn_name to client change by jessica at 20200605
                                --v_count || '期:' || v_result_col_list || '"' ||
                                v_count || '期:' || v_cn_result_col_list || '"' ||
                                ' VALUE=' || '"' || v_result_a_list_detail || '"' ||
                                ' BEFORE_VALUE=' || '"' ||
                                v_result_b_list_detail || '"' || '  USER=' || '"' ||
                                v_check_user || '"' || ' DATE=' || '"' ||
                                v_check_date || '" />';
                end if;
				if nvl(is_redone_pri,99) < is_redone_pri_save  and is_redone_pri is not null then 
				    is_redone_pri_save :=  nvl(is_redone_pri,99);
					lc_datastr_redone := lc_datastr;
				end if;
				if is_redone_pri_save=1 then
                   lc_ret.extend;
                   lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr_redone);
                   lc_int := lc_int + 1;                
                   v_find_need := 1;
				   exit;
                end if;
              end if;

            end loop;
			
			if is_redone_pri_save is not null and is_redone_pri_save>1 then
			   lc_ret.extend;
               lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr_redone);
               lc_int := lc_int + 1;                
               v_find_need := 1;
			end if;

            /*期数不一样提示：期数变动*/
            if (v_seq_before <> v_seq_now) then
              v_find_need := 1;
              v_now_count  := lpad(v_seq_now, 3, '0');
              v_last_count := lpad(v_seq_before, 3, '0');
              if (in_mail = 1) then
                lc_datastr := '凭证代码:' || security_rec.security_code ||
                              ' 凭证简称:' || v_security_name ||
                              ' 类型:重算字段 修改字段:期数变动 现数据:' || v_now_count ||
                              '期 原数据:' || v_last_count || '期 审核人:' ||
                              v_check_user || ' 审核通过时间:' || v_check_date;
              else
                lc_datastr := '<DATA TYPE=' || '"' || in_type || '"' ||
                              ' SECURITY_CODE=' || '"' ||
                              security_rec.security_code || '"' ||
                              ' SECURITY_NAME=' || '"' || v_security_name || '"' ||
                              ' IS_REDONE=' || '"Y"' ||
                              ' COLNAME="期数变动" VALUE=' || '"' ||
                              v_now_count || '期"' || ' BEFORE_VALUE=' || '"' ||
                              v_last_count || '期"' || '  USER=' || '"' ||
                              v_check_user || '"' || ' DATE=' || '"' ||
                              v_check_date || '" />';
              end if;
              lc_ret.extend;
              lc_ret(lc_ret.last) := out_row_entry_r(lc_int, lc_datastr);
              lc_int := lc_int + 1;
            end if;

          end if;

        end if;
      end loop;

    end loop;

  end if;
  return lc_ret;
end get_redone_security_crmw;
/
