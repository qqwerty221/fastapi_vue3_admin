export interface searchDataType {
    app_name?: string
    script_name?: string
    is_parsed?: string
}

export interface tableDataType {
    id?: number;
    app_name?: string;
    script_name?: string;
    script_path?: string;
    script_content?: string;
    is_deleted?: boolean;
    is_parsed?: boolean;
    create_at?: string;
    updated_at?: string;
    creator_id?: number;
    creator?: creatorType;


}

interface creatorType {
    id?: number;
    name?: string;
    username?: string;
}