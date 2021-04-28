import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NamedSqlUtil {
    /**
     * 将具名参数写法的sql，转换成占位符写法，整个方法只会循环遍历一遍 targetSql
     * 占位符key写法“:key”，例：select * from table where name = #:name  （“key名仅支持 大小写英文及数字”）
     * list写法“#{listname:and #}”，例：select * from table where #{aList:or (name = #:name and year = #:year)#}  （“冒号”后可跟任何符号或字符串的间隔符，但是最后都得用空格与正文隔开）
     * 解析后：select * from table where (name = ? and year = ?) or (name = ? and year = ?) or (name = ? and year = ?)
     * @param targetSql 含有具名参数sql
     * @param paramMap 参数map
     * @param resSql 存放转换结果sql
     * @param resPrmList 存放sql对应的占位符结果集
     * @author Chen768959
     * @return void
     */
    public void namedPrmToPreparedPrm(String targetSql, Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList) throws Exception{
        // 是否在解析变量
        boolean analyseKey = false;
        // 当前解析的参数key
        StringBuilder curKey = new StringBuilder();

        // 是否在解析列表
        boolean analyseList = false;
        // 当前解析列表的列表key
        StringBuilder curListKey = new StringBuilder();
        // 当前解析列表的循环时间隔条件
        StringBuilder curListType = new StringBuilder();

        try {
            StringCharacterIterator targetSqlIterator = new StringCharacterIterator(targetSql);
            while (targetSqlIterator.current() != StringCharacterIterator.DONE){
                // 尝试解析变量
                if (analyseKey = analyseKey(targetSqlIterator.current(), analyseKey, curKey, paramMap, resSql, resPrmList, targetSqlIterator)){
                    targetSqlIterator.next();
                    continue;
                }
                // 尝试解析列表
                if (analyseList = analyseList(targetSqlIterator.current(), analyseList, curListKey, curListType, paramMap, resSql, resPrmList, targetSqlIterator)){
                    targetSqlIterator.next();
                    continue;
                }
                // 否则无需解析直接存入
                resSql.append(targetSqlIterator.current());
                targetSqlIterator.next();
            }
        }catch (Exception e){
            throw e;
        }

        // 末尾字符为占位符
        if (analyseKey){
            Object value = paramMap.get(curKey.toString());
            if (value == null){
                // log
            }
            resPrmList.add(value);
            resSql.append("? ");
        }
    }

    /**
     * 解析循环体
     * @param c 当前字符
     * @param analyseList 是否在解析循环体列表
     * @param curListKey 循环体列表当前的key名，{nameKey:or }中的nameKey
     * @param curListType 循环体列表当前的间隔符，{nameKey:or }中的or
     * @param paramMap 总参数
     * @param resSql
     * @param resPrmList
     * @param targetSqlIterator
     * @author Chen768959
     * @return boolean
     */
    private boolean analyseList(char c, boolean analyseList, StringBuilder curListKey, StringBuilder curListType,
                                Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList,
                                StringCharacterIterator targetSqlIterator) {
        // 开始解析
        if(analyseList){
            // 开始解析listKsy和listType
            if (curListKey.length() == 0){
                curListKey.append(c);
                // 遍历到空格为止，期间解析出listKey和listType
                while (! Character.isSpaceChar(c = targetSqlIterator.next())){
                    if (c == ':'){// 开始解析listType
                        while (! Character.isSpaceChar(c = targetSqlIterator.next())){// 顺着当前index继续遍历
                            curListType.append(c);
                        }
                        // 遍历到空格后，结束遍历
                        return true;
                    }
                    curListKey.append(c);
                }
                return true;
            }

            // 开始解析需要循环内容
            StringBuilder loopStrBuilder = new StringBuilder();// 循环内容，其中占位符会被解析成'?'，
            List<String> keyInloopStrList = new ArrayList<>();// 循环内容中如果还包含具名参数，则key名依次被解析到此list
            boolean analyseKeyInLoop = false;
            StringBuilder keyInLoop = new StringBuilder();

            // 遍历循环体直至结尾
            while (! checkTargetTagCur('#','}',targetSqlIterator)){
                // 解析loop内的具名key
                if (analyseKeyInLoop){
                    if( ! (Character.isLowerCase(c) || Character.isUpperCase(c) || Character.isDigit(c))){// KeyInLoop解析完毕
                        analyseKeyInLoop = false;
                        keyInloopStrList.add(keyInLoop.toString());
                        loopStrBuilder.append("?").append(c);
                    }

                    keyInLoop.append(c);
                    c = targetSqlIterator.next();
                    continue;
                }

                // 准备解析loop内的具名key
                if (checkTargetTagCur('#',':',targetSqlIterator)){
                    analyseKeyInLoop = true;
                    keyInLoop.delete(0,keyInLoop.length());
                    c = targetSqlIterator.next();//next之前下标指向':'，next后指向loop内key名首字符
                    continue;
                }

                //正常loop内容
                loopStrBuilder.append(c);
                c = targetSqlIterator.next();
            }

            // 如果循环体解析完毕，还存在正在解析的内部具名key，则该具名key存在于末尾处
            if (analyseKeyInLoop){
                keyInloopStrList.add(keyInLoop.toString());
                loopStrBuilder.append("?");
            }

            // loop解析完毕，根据参数列表构建多个loopStr
            List<Map<String, Object>> loopParamList = (List<Map<String, Object>>) paramMap.get(curListKey.toString());
            if (loopParamList == null){
                // log
                return false;
            }
            loopParamList.stream().forEach(loopParamMap->{
                // 构建结构sql
                resSql.append(' ').append(loopStrBuilder).append(' ').append(curListType);

                // 构建结果sql的对应占位符结果list
                keyInloopStrList.stream().forEach(keyInloopStr->{
                    Object resPrm = loopParamMap.get(keyInloopStr);
                    if (resPrm == null){
                        // log
                    }
                    resPrmList.add(resPrm);
                });
            });
            // 删除末尾curListType
            if (! loopParamList.isEmpty()){
                resSql.delete(resSql.length() - curListType.length(), resSql.length());
            }

            // list解析完毕,index后移1，因为当前index值为“}”
            targetSqlIterator.next();
        }

        //准备解析list
        if (checkTargetTagCur('#', '{', targetSqlIterator)){
            curListKey.delete(0,curListKey.length());
            curListType.delete(0,curListType.length());
            return true;
        }

        return false;
    }

    /**
     * 解析变量
     * @param c 当前字符
     * @param analyseKey
     * @param curKey
     * @param paramMap
     * @param resSql
     * @param resPrmList
     * @author Chen768959
     * @return boolean true表示正在解析，false表示该字符不涉及变量解析
     */
    private boolean analyseKey(char c, boolean analyseKey, StringBuilder curKey, Map<String,Object> paramMap,
                               StringBuilder resSql, List<Object> resPrmList, StringCharacterIterator targetSqlIterator) {
        // curKey非空则当前处于解析key中，判断是否为空格，空格则表示解析结束
        if (analyseKey){
            if (Character.isLowerCase(c) || Character.isUpperCase(c) || Character.isDigit(c)){
                curKey.append(c);
                return true;
            }else {
                // 根据现有key获取参数value，添加？占位符
                Object value = paramMap.get(curKey.toString());
                if (value == null){
                    // log
                }

                resPrmList.add(value);
                resSql.append("?");

                return false;
            }
        }

        // 准备解析key
        if (checkTargetTagCur('#',':',targetSqlIterator)){
            curKey.delete(0,curKey.length());
            return true;
        }

        return false;
    }

    /**
     * 检查当前读取位置是否满足指定顺序字符，
     * 如果满足，则下标位移到第二个字符
     * 如果不满足，则保持不变
     * @param targetOne 需要满足的第一个字符
     * @param targetTwo 需要满足的第二个字符
     * @param targetSqlIterator
     * @author Chen768959
     * @return boolean
     */
    private boolean checkTargetTagCur(char targetOne, char targetTwo, StringCharacterIterator targetSqlIterator){
        if (targetSqlIterator.current() == targetOne){
            if (targetSqlIterator.next() == targetTwo){
                return true;
            }else {
                targetSqlIterator.previous();
                return false;
            }
        }
        return false;
    }
}
