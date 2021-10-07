CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$

    DECLARE 
        data json;
        newdata json;
        olddata json;
        notification json;
    
    BEGIN
    
        -- Convert the old or new row to JSON, based on the kind of action.
        -- Action = DELETE?             -> OLD row
        -- Action = INSERT or UPDATE?   -> NEW row
        IF (TG_OP = 'DELETE') THEN
            data = row_to_json(OLD);
            notification = json_build_object(
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'new', null,
                          'old', data);
        ELSIF (TG_OP = 'INSERt') THEN
            data = row_to_json(NEW);
            notification = json_build_object(
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'new', data,
                          'old', null);
        ELSE
            newdata = row_to_json(NEW);
            olddata = row_to_json(OLD);
            notification = json_build_object(
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'new', newdata,
                          'old', olddata);
        END IF;
        
        -- Execute pg_notify(channel, notification)
        PERFORM pg_notify('events',notification::text);
        
        -- Result is ignored since this is an AFTER trigger
        RETURN NULL; 
    END;
    
$$ LANGUAGE plpgsql;


CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dagl_person
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dagl_bodycheck
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dagl_bodycheck_second
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dagl_bodycheck_third
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON dagl_record_gxy
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON fd_contacts
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON fd_contract_packs
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON fd_packages
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON mxb_gxysf
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON mxb_personnr
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON old_healthmanagement
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON sys_department
FOR EACH ROW EXECUTE PROCEDURE notify_event();

CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE ON sys_townvillage
FOR EACH ROW EXECUTE PROCEDURE notify_event();


