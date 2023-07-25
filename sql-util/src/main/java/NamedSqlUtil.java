import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Chen768959
 * @date 2023/7/13
 */
public class NamedSqlUtil {
    /**
     * 将具名参数写法的sql，转换成占位符写法
     *
     * 支持如下特殊关键字：
     * 1、字符串替换
     * #={strKey}
     * strKey作为key，从"paramMap"中查找value替换；
     * strKey仅支持大小写英文及数字。
     *
     * 2、字符串替换成占位符
     * #:{strKey}
     * '#:{strKey}'替换为'?'，strKey作为key，从map中查找value作为填充Object；
     * strKey仅支持大小写英文及数字。
     *
     * 3、if条件关键字
     * #{IF={strKey} 待解析逻辑}
     * strKey作为key，从map中查找value；
     * 如果value存在，则继续解析if中的内容，如不存在，则忽略if中内容；
     * strKey仅支持大小写英文及数字。
     *
     * 4、list循环关键字
     * #{listName:type 待循环逻辑}
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
     * @param resPrmList 存放sql对应的占位符结果集，
     *                   如：param key: "user_name" param value: "小明"
     *                   解析前 "select v1 from table where v1 = #:{user_name}"
     *                   解析后 "select v1 from table where v1 = ?"   且   List<Object> resPrmList 中存一项String "小明"
     *                   resPrmList中的数据顺序，即为解析后sql中对应占位符顺序
     * @author Chen768959
     * @return void
     */
    public static void namedPrmToPreparedPrm(String targetSql, Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList){
        StringCharacterIterator targetSqlIterator = new StringCharacterIterator(targetSql);
        while (targetSqlIterator.current() != StringCharacterIterator.DONE){
            checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);

            resSql.append(targetSqlIterator.current());
            targetSqlIterator.next();
        }

        // 末尾EOF需去除
        if (resSql.charAt(resSql.length()-1) == StringCharacterIterator.DONE){
            resSql.deleteCharAt(resSql.length()-1);
        }
    }

    // 无需生成占位符结果集时，可调用此方法
    public static String namedPrmToPreparedPrm(String targetSql, Map<String,Object> paramMap){
        StringBuilder resSql = new StringBuilder();
        namedPrmToPreparedPrm(targetSql, paramMap, resSql, null);
        return resSql.toString();
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
    private static boolean checkSpecialAndAct(StringCharacterIterator targetSqlIterator, Map<String,Object> paramMap, StringBuilder resSql, List<Object> resPrmList){
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
                if (checkTargetTagCur(targetSqlIterator, '{')){
                    analysePlaceholderLogic(targetSqlIterator, paramMap, resSql, resPrmList);
                    checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
                    return true;
                }
            }
            if (checkTargetTagCur(targetSqlIterator, '=')){ // 满足字符串替换条件
                if (checkTargetTagCur(targetSqlIterator, '{')){
                    analyseReplaceLogic(targetSqlIterator, paramMap, resSql);
                    checkSpecialAndAct(targetSqlIterator, paramMap, resSql, resPrmList);
                    return true;
                }
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
    private static boolean checkTargetTagCur(StringCharacterIterator targetSqlIterator, char... targetChar){
        for (int i=0 ; i<targetChar.length; i++){
            if (targetSqlIterator.current() != targetChar[i]){
                for ( ; i > 0 ; i--){
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
     * 当前下标指向'#={'后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @author Chen768959
     * @return void
     */
    private static void analyseReplaceLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql) {
        String strKey = traveToSpecial(targetSqlIterator);

        Object resValue = paramMap.get(strKey);
        if (resValue == null){
            throw  new IllegalArgumentException("NamedSqlError : analyseReplaceLogic error, strKey not found");
        }

        resSql.append(resValue);
    }

    /**
     * 解析占位符条件，
     * 当前下标指向'#:{'后一位
     * @param targetSqlIterator
     * @param paramMap
     * @param resSql
     * @param resPrmList
     * @author Chen768959
     * @return void
     */
    private static void analysePlaceholderLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {
        String strKey = traveToSpecial(targetSqlIterator);

        Object resValue = paramMap.get(strKey);
        if (resValue == null){
            throw  new IllegalArgumentException("NamedSqlError : analysePlaceholderLogic error, strKey not found");
        }

        resSql.append('?');
        if (resPrmList != null){
            resPrmList.add(resValue);
        }
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
    private static void analyseIfLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {
        int loopNum = 0;

        // 判断if的strKey是否存在
        String strKey = traveToSpecial(targetSqlIterator);
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
                        c = targetSqlIterator.next();//跳出if，且将下标移向if后一位
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
                    c = targetSqlIterator.next();//跳出if，且将下标移向if后一位
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
    private static void analyseListLogic(StringCharacterIterator targetSqlIterator, Map<String, Object> paramMap, StringBuilder resSql, List<Object> resPrmList) {
        char c = targetSqlIterator.current();
        StringBuilder listKey = new StringBuilder(); // listKey
        StringBuilder intervalStr = new StringBuilder(); // 间隔符
        StringBuilder loopStrBuilder = new StringBuilder(); // 循环体原生内容

        // 解析listKey
        while ( c != ':' && c != StringCharacterIterator.DONE){
            listKey.append(c);
            c = targetSqlIterator.next();
        }

        List<Map<String, Object>> loopParamList;
        Object listObj = paramMap.get(listKey.toString());
        if (listObj == null){
            throw  new IllegalArgumentException("NamedSqlError : analyseListLogic error, listKey not found");
        }else if (listObj instanceof List) {
            loopParamList = (List<Map<String, Object>>) paramMap.get(listKey.toString());
        }else {
            throw  new IllegalArgumentException("NamedSqlError : analyseListLogic error, listKey value not is list, listKey="+listKey.toString());
        }

        // 解析间隔符
        while ((! Character.isSpaceChar(c = targetSqlIterator.next())) && c != StringCharacterIterator.DONE){
            intervalStr.append(c);
        }

        // 此时下标指向间隔符后一位空格处
        // 解析循环体（此时保留原生内容）
        int loopNum = 0;
        while (c != StringCharacterIterator.DONE){
            if (c == '{'){
                loopNum++;
            }else if (c == '}'){
                if (loopNum <= 0){
                    c = targetSqlIterator.next();//跳出list，且将下标移向list后一位
                    break;
                }else {
                    loopNum--;
                }
            }

            loopStrBuilder.append(c);
            c = targetSqlIterator.next();
        }
        // 循环体解析完毕
        String loopStr = loopStrBuilder.toString();
        // 根据map数循环list
        Optional.ofNullable(loopParamList).orElse(new ArrayList<>()).forEach(map -> {
            // 每一次循环的参数集是当前map对象，作用的原始sql域则是此次的循环体，但是总的结果sql和结果填充list都没变
            StringCharacterIterator loopIterator = new StringCharacterIterator(loopStr);
            while (loopIterator.current() != StringCharacterIterator.DONE){
                checkSpecialAndAct(loopIterator, map, resSql, resPrmList);

                if (loopIterator.current() != StringCharacterIterator.DONE){
                    resSql.append(loopIterator.current());
                    loopIterator.next();
                }
            }

            //结尾处加上间隔符
            resSql.append(' ').append(intervalStr);
        });

        // 清除末尾间隔符
        if ( ! Optional.ofNullable(loopParamList).orElse(new ArrayList<>()).isEmpty()){
            resSql.delete(resSql.length() - intervalStr.length(), resSql.length());
        }
    }

    /**
     * 从当前下标开始遍历直到特殊字符，返回遍历内容
     * @param targetSqlIterator
     * @author Chen768959
     * @return java.lang.String
     */
    private static String traveToSpecial(StringCharacterIterator targetSqlIterator){
        char c = targetSqlIterator.current();
        StringBuilder strKey = new StringBuilder();

        //判断if的strKey是否存在，不存在则忽略if内容
        while ( (Character.isLowerCase(c) || Character.isUpperCase(c) || Character.isDigit(c)) && c != StringCharacterIterator.DONE ){
            strKey.append(c);
            c = targetSqlIterator.next();
        }

        if (c=='}'){
            targetSqlIterator.next();
        }else {
            throw  new IllegalArgumentException("NamedSqlError : targetSql strKey not found '}', strKey is " + strKey);
        }

        return strKey.toString();
    }
}