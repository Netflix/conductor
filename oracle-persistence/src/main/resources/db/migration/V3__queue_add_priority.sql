--
-- Copyright 2020 Netflix, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

DECLARE
    sql_query  VARCHAR2 (150);
BEGIN
    SELECT CASE WHEN 
        (SELECT COUNT(*) FROM all_tab_columns WHERE OWNER = USER AND TABLE_NAME = 'QUEUE_MESSAGE' AND COLUMN_NAME = 'PRIORITY' ) > 0 
        THEN
            'SELECT 1 FROM DUAL'
        ELSE
            'ALTER TABLE QUEUE_MESSAGE ADD PRIORITY NUMBER(3) DEFAULT 0'
        END
    INTO sql_query from DUAL;
    EXECUTE IMMEDIATE sql_query;
END;
/
