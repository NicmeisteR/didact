jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $leaders := jn:parse-json(file:read-text("./meta/data/leaders.json"))."ContentItems"

return [
    for $leader in $leaders[]
    where $leader."Type" = "HW2Leader"
    order by $leader."View"."HW2Leader"."Name"
    return [
        $leader."View"."HW2Leader"."Id",
        $leader."View"."HW2Leader"."DisplayInfo"."View"."HW2LeaderDisplayInfo"."Name",
        $leader."View"."HW2Leader"."Faction",
        $leader."View"."HW2Leader"."Image"."View"."Media"."MediaUrl"
    ]
]
