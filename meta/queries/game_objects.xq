jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $objects_1 := jn:parse-json(file:read-text("./meta/data/game_objects_1.json"))."ContentItems"
let $objects_2 := jn:parse-json(file:read-text("./meta/data/game_objects_2.json"))."ContentItems"
let $objects_3 := jn:parse-json(file:read-text("./meta/data/game_objects_3.json"))."ContentItems"
let $objects_4 := jn:parse-json(file:read-text("./meta/data/game_objects_4.json"))."ContentItems"
let $objects := [$objects_1[], $objects_2[], $objects_3[], $objects_4[]]

return {
    game_objects: [
        for $object in $objects[]
        where $object."Type" = "HW2Object"
        order by $object."View"."HW2Object"."ObjectTypeId"
        return {
            uuid: $object."View"."Identity",
            name: $object."View"."HW2Object"."ObjectTypeId"
        }
    ]
}
