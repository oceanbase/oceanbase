#include "lib/file/config.h"
#include "lib/oblog/ob_log.h"
using namespace std;

namespace obsys {

    static ObSysConfig _config;

    ObSysConfig::ObSysConfig()
    {
    }

    ObSysConfig::~ObSysConfig()
    {
        for(STR_MAP_ITER it=m_configMap.begin(); it!=m_configMap.end(); ++it) {
            delete it->second;
        }
    }
    
    ObSysConfig& ObSysConfig::getCConfig()
    {
        return _config;
    }

    /**
     * 解析字符串
     */
    int ObSysConfig::parseValue(char *str, char *key, char *val)
    {
        char           *p, *p1, *name, *value;

        if (str == NULL)
            return -1;

        p = str;
        // Remove leading spaces
        while ((*p) == ' ' || (*p) == '\t' || (*p) == '\r' || (*p) == '\n') p++;
        p1 = p + strlen(p);
        // Remove trailing spaces
        while(p1 > p) {
            p1 --;
            if (*p1 == ' ' || *p1 == '\t' || *p1 == '\r' || *p1 == '\n') continue;
            p1 ++;
            break;
        }
        (*p1) = '\0';
        // Is a comment line or blank line
        if (*p == '#' || *p == '\0') return -1;
        p1 = strchr(str, '=');
        if (p1 == NULL) return -2;
        name = p;
        value = p1 + 1;
        while ((*(p1 - 1)) == ' ') p1--;
        (*p1) = '\0';

        while ((*value) == ' ') value++;
        p = strchr(value, '#');
        if (p == NULL) p = value + strlen(value);
        while ((*(p - 1)) <= ' ') p--;
        (*p) = '\0';
        if (name[0] == '\0')
            return -2;

        STRCPY(key, name);
        STRCPY(val, value);

        return 0;
    }

    int ObSysConfig::parseLine(STR_STR_MAP *&m, char *data)
    {
        int             ret = 0;
        char            key[128], value[4096];
        char          * sName = isSectionName(data);
        // Is the segment name
        if (sName != NULL) {
            STR_MAP_ITER it = m_configMap.find(sName);
            if (it == m_configMap.end()) {
                m = new STR_STR_MAP();
                m_configMap.insert(STR_MAP::value_type(/*ObStringUtil::str_to_lower(sName)*/sName, m));
            } else {
                m = it->second;
            }
        } else {
            ret = parseValue(data, key, value);
            if (ret == -2) {
                _OB_LOG(ERROR, "解析错误, Line: %s", data);
            } else if (ret < 0) {
                ret = 0;
            } else if (m == NULL) {
                _OB_LOG(ERROR, "没在配置section, Line: %s", data);
                ret = -1;
            } else {
                STR_STR_MAP_ITER it1 = m->find(key);
                if (it1 != m->end()) {
                    it1->second += '\0';
                    it1->second += value;
                } else {
                    m->insert(STR_STR_MAP::value_type(key, value));
                }
            }
        }
        return ret;
    }

    /* Is the segment name */
    char *ObSysConfig::isSectionName(char *str) {
        if (str == NULL || strlen(str) <= 2 || (*str) != '[')
            return NULL;

        char *p = str + strlen(str);
        while ((*(p-1)) == ' ' || (*(p-1)) == '\t' || (*(p-1)) == '\r' || (*(p-1)) == '\n') p--;
        if (*(p-1) != ']') return NULL;
        *(p-1) = '\0';

        p = str + 1;
        while(*p) {
            if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || (*p == '_')) {
            } else {
                return NULL;
            }
            p ++;
        }
        return (str+1);
    }

    /**
     * 加载文件
     */
    int ObSysConfig::load(const char *filename)
    {
        FILE           *fp;
        int             ret;
        char            data[4096];

        if ((fp = fopen(filename, "rb")) == NULL) {
            _OB_LOG(ERROR, "不能打开配置文件: %s", filename);
            return EXIT_FAILURE;
        }

        STR_STR_MAP *m = NULL;
        while (fgets(data, 4096, fp)) {
            ret = parseLine(m, data);
            if (ret != 0) {
                _OB_LOG(ERROR, "parseLine error: %s", data);
                break;
            }
        }
        fclose(fp);
        return ret;
    }

    int ObSysConfig::loadContent(const char * content)
    {
        int             ret = 0;
        char            data[4096];

        int             content_len = static_cast<int>(strlen(content)), pos = 0;

        STR_STR_MAP *m = NULL;
        if (content_len > 0) {
            do {
                ret = getLine(data, 4096, content, content_len, pos);
                if (ret != 0) {
                    _OB_LOG(ERROR, "getLine error: %d, %s", pos, content);
                    break;
                } else {
                    ret = parseLine(m, data);
                    if (ret != 0) {
                        _OB_LOG(ERROR, "parseLine error: %s", data);
                        break;
                    }
                }
            } while (pos < content_len);
        } else {
            _OB_LOG(INFO, "content is empty");
        }
        return ret;
    }

    int ObSysConfig::getLine(char * buf, const int buf_len,
        const char * content, const int content_len, int & pos)
    {
        int ret = 0;
        if (pos < content_len) {
            int end = pos;
            while (end < content_len && content[end] != '\n') end++;
            if (end - pos >= buf_len) {
                _OB_LOG(ERROR, "line size exceed max: %d %d", end - pos, buf_len);
                ret = -1;
            } else {
                memcpy(buf, content + pos, end - pos);
                buf[end - pos] = '\0';
                if (end == content_len) {
                    pos = end;
                } else {
                    pos = end + 1;
                }
            }
        } else {
            _OB_LOG(ERROR, "error pos: %d", pos);
            ret = -1;
        }
        return ret;
    }

    /**
     * 取一个string
     */
    const char *ObSysConfig::getString(const char *section, const string& key, const char *d)
    {
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return d;
         }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return d;
        }
        return it1->second.c_str();
    }

    /**
     * 取一string列表
     */
    vector<const char*> ObSysConfig::getStringList(const char *section, const string& key) {
        vector<const char*> ret;
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return ret;
        }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return ret;
        }
        const char *data = it1->second.data();
        const char *p = data;
        for(int i=0; i<(int)it1->second.size(); i++) {
            if (data[i] == '\0') {
                ret.push_back(p);
                p = data+i+1;
            }
        }
        ret.push_back(p);
        return ret;
    }

    /**
     * 取一整型
     */
    int ObSysConfig::getInt(const char *section, const string& key, int d)
    {
        const char *str = getString(section, key);
        return ObStringUtil::str_to_int(str, d);
    }

    /**
     * 取一int list
     */
    vector<int> ObSysConfig::getIntList(const char *section, const string& key) {
        vector<int> ret;
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return ret;
        }
        STR_STR_MAP_ITER it1 = it->second->find(key);
        if (it1 == it->second->end()) {
            return ret;
        }
        const char *data = it1->second.data();
        const char *p = data;
        for(int i=0; i<(int)it1->second.size(); i++) {
            if (data[i] == '\0') {
                ret.push_back(atoi(p));
                p = data+i+1;
            }
        }
        ret.push_back(atoi(p));
        return ret;
    }

    // Take all the keys under a section
    int ObSysConfig::getSectionKey(const char *section, vector<string> &keys)
    {
        STR_MAP_ITER it = m_configMap.find(section);
        if (it == m_configMap.end()) {
            return 0;
        }
        STR_STR_MAP_ITER it1;
        for(it1=it->second->begin(); it1!=it->second->end(); ++it1) {
            keys.push_back(it1->first);
        }
        return (int)keys.size();
    }

    // Get the names of all sections
    int ObSysConfig::getSectionName(vector<string> &sections)
    {
        STR_MAP_ITER it;
        for(it=m_configMap.begin(); it!=m_configMap.end(); ++it)
        {
            sections.push_back(it->first);
        }
        return (int)sections.size();
    }

    // toString
    string ObSysConfig::toString()
    {
        string result;
        STR_MAP_ITER it;
    	STR_STR_MAP_ITER it1;
    	for(it=m_configMap.begin(); it!=m_configMap.end(); ++it) {
            result += "[" + it->first + "]\n";
    	    for(it1=it->second->begin(); it1!=it->second->end(); ++it1) {
    	        string s = it1->second.c_str();
                result += "    " + it1->first + " = " + s + "\n";
                if (s.size() != it1->second.size()) {
                    char *data = (char*)it1->second.data();
                    char *p = NULL;
                    for(int i=0; i<(int)it1->second.size(); i++) {
                        if (data[i] != '\0') continue;
                        if (p) result += "    " + it1->first + " = " + p + "\n";
    	                p = data+i+1;
    	            }
    	            if (p) result += "    " + it1->first + " = " + p + "\n";
    	        }
            }
        }
        result += "\n";
        return result;
    }
}
///////////////
