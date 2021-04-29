import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NamedSqlUtil {
    /**
     * 将具名参数写法的sql，转换成占位符写法，整个方法只会循环遍历一遍 targetSql
     *
     * 支持如下特殊关键字：
     * 1、字符串替换
     * #=strKey
     * strKey作为key，从map中查找value替换；
     * strKey仅支持大小写英文及数字。
     *
     * 2、字符串替换成占位符
     * #:strKey
     * '#:strKey'替换为'?'，strKey作为key，从map中查找value作为填充Object；
     * strKey仅支持大小写英文及数字。
     *
     * 3、if条件关键字
     * #{IF=strKey }
     * strKey作为key，从map中查找value；
     * 如果value存在，则继续解析if中的内容，如不存在，则忽略if中内容；
     * strKey仅支持大小写英文及数字。
     *
     * 4、list循环关键字
     * #{listName:type }
     * listName作为key，从map中查找value；
     * 循环此中内容，每次循环会将'type'作为间隔符；
     * 此value应为 List<Map<String, Object>> 类型；
     * list中的map数量决定了循环的次数，每个map即每次循环时会用到的参数map；
     * listName仅支持大小写英文及数字；
     * type支持“除空格以外”的任意字符串，单需用空格与正文隔开；
     * list中也同样支持此四种关键字。
     *
     * 例：select * from table where #{aList:or (name = #:name and year = #:year)}
     * 解析后：select * from table where (name = ? and year = ?) or (name = ? and year = ?) or (name = ? and year = ?)
     *
     * @param targetSql 含有具名参数sql
     * @param paramMap 参数map
     * @param resSql 存放转换结果sql
     * @param resPrmList 存放sql对应的占位符结果集
     * @author Chen768959
     * @return void
     */
    public void namedPrmToPreparedPrm(String targetSql, Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList) throws Exception{
        try {
            StringCharacterIterator targetSqlIterator = new StringCharacterIterator(targetSql);
            while (targetSqlIterator.current() != StringCharacterIterator.DONE){
                checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);

                resSql.append(targetSqlIterator.current());
                targetSqlIterator.next();
            }
        }catch (Exception e){
            logger.error("namedPrmToPreparedPrm解析异常，检查传参格式",e);
            throw e;
        }
    }

    /**
     * 检查当前字符是否是特殊字符（#{IF、#{、#:、#=）
     * 如果匹配成功，
     * 则进入对应解析方法，且填充好resSql和resPrmList，
     * 最后指标下移到后一位“未处理，且非特殊字符”的下标（即解析完后检查下一位字符，如果依旧为特殊字符则递归直到指向正常字符）
     * 且返回true
     *
     * 如果未匹配成功，则返回false且保持当前字符指针不动
     *
     * 总之该方法完毕后总会指向“非特殊字符”的下标
     * @param targetSqlIterator
     * @author Chen768959
     * @return boolean
     */
    private boolean checkSpecialAndAct(StringCharacterIterator targetSqlIterator, Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList){
        if (checkTargetTagCur(targetSqlIterator, '#')){
            if (checkTargetTagCur(targetSqlIterator, '{')){
                if (checkTargetTagCur(targetSqlIterator,'I','F','=')){ // 满足if条件
                    analyseIfLogic(targetSqlIterator, paramMap, resSql, resPrmList);
                }else { // 满足list条件
                    analyseListLogic(targetSqlIterator, paramMap, resSql, resPrmList);
                }
                checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
                return true;
            }
            if (checkTargetTagCur(targetSqlIterator, ':')){ // 满足占位符替换条件
                analysePlaceholderLogic(targetSqlIterator, paramMap, resSql, resPrmList);
                checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
                return true;
            }
            if (checkTargetTagCur(targetSqlIterator, '=')){ // 满足字符串替换条件
                analyseReplaceLogic(targetSqlIterator, paramMap, resSql);
                checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
                return true;
            }
        }

        return false;
    }

    /**
     * 判断当前读取位置是否满足指定顺序的字符，
     * 满足则下标位移到“指定字符后的一位”
     * 不满足则不做任何位移
     * @param targetSqlIterator
     * @param targetChar
     * @author Chen768959
     * @return boolean 满足：true，不满足：false
     */
    private boolean checkTargetTagCur(StringCharacterIterator targetSqlIterator, char... targetChar){
        for (int i=0 ; i<targetChar.length; i++){
            if (targetSqlIterator.current() != targetChar[i]){
                for (int j=i ; j<=0 ; j--){
                    targetSqlIterator.previous();
                }
                return false;
            }
            targetSqlIterator.next();
        }

        return true;
    }

    /**
     * 解析字符串替换条件，
     * 当前下标指向'#='后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @author Chen768959
     * @return void
     */
    private void analyseReplaceLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql) {
        String strKey = traveToSpace(targetSqlIterator);

        Object resValue = paramMap.get(strKey);
        if (resValue == null){
            logger.warn("analyseReplaceLogic失败，key不存在："+strKey);
        }

        resSql.append(resValue);
    }

    /**
     * 解析占位符条件，
     * 当前下标指向'#:'后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @param resPrmList
     * @author Chen768959
     * @return void
     */
    private void analysePlaceholderLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {
        String strKey = traveToSpace(targetSqlIterator);

        Object resValue = paramMap.get(strKey);
        if (resValue == null){
            logger.warn("analysePlaceholderLogic失败，key不存在："+strKey);
        }

        resSql.append('?');
        resPrmList.add(resValue);
    }

    /**
     * 解析if条件，
     * 当前下标指向'#{IF='后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @param resPrmList
     * @author Chen768959
     * @return void
     */
    private void analyseIfLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {
        int loopNum = 0;

        // 判断if的strKey是否存在
        String strKey = traveToSpace(targetSqlIterator);
        Object resValue = paramMap.get(strKey);

        // 当前指针指向if的strKey的后一位

        // 对应value不存在则忽略if内容
        if (resValue == null){
            char c = targetSqlIterator.current();
            while (c != StringCharacterIterator.DONE){
                if (c == '{'){
                    loopNum++;
                }else if (c == '}'){
                    if (loopNum <= 0){
                        targetSqlIterator.next();//跳出if，且将下标移向if后一位
                        break;
                    }else {
                        loopNum--;
                    }
                }

                c = targetSqlIterator.next();
            }
            return;
        }

        // 解析if中内容
        char c = targetSqlIterator.current();
        while (c != StringCharacterIterator.DONE){
            // 判断当前符号是否为特殊关键字，是则需要解析
            checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
            // 重新赋值解析后位置
            c = targetSqlIterator.current();
            if (c == '{'){
                loopNum++;
            }else if (c == '}'){
                if (loopNum <= 0){
                    targetSqlIterator.next();//跳出if，且将下标移向if后一位
                    break;
                }else {
                    loopNum--;
                }
            }

            resSql.append(c);
            c = targetSqlIterator.next();
        }
    }

    /**
     * 解析list条件，
     * 当前下标指向'#{'后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @param resPrmList
     * @author Chen768959
     * @return void
     */
    private void analyseListLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {

    }

    /**
     * 从当前下标开始遍历直到特殊字符，返回遍历内容
     * @param targetSqlIterator
     * @author Chen768959
     * @return java.lang.String
     */
    private String traveToSpace(StringCharacterIterator targetSqlIterator){
        char c = targetSqlIterator.current();
        StringBuilder strKey = new StringBuilder();

        //判断if的strKey是否存在，不存在则忽略if内容
        while ( Character.isLowerCase(c) || Character.isUpperCase(c) || Character.isDigit(c) ){
            strKey.append(c);
            c = targetSqlIterator.next();
        }

        return strKey.toString();
    }
}