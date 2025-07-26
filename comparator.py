from deepdiff import DeepDiff

def compare_metadata(source_meta, dest_meta, config, logger):
    result = {}
    for obj_type in source_meta:
        src_objs = source_meta.get(obj_type, {})
        dst_objs = dest_meta.get(obj_type, {})

        diffs = {
            "missing_in_dest": [],
            "extra_in_dest": [],
            "mismatched": []
        }

        for key in src_objs:
            if key not in dst_objs:
                diffs["missing_in_dest"].append(key)
            else:
                diff = DeepDiff(src_objs[key], dst_objs[key], ignore_order=True, view='tree')
                side_by_side = []

                for diff_group in diff.values():
                    for diff_item in diff_group:
                        try:
                            side_by_side.append({
                                "attribute": str(diff_item.path()),
                                "source": diff_item.t1,
                                "destination": diff_item.t2
                            })
                        except Exception as e:
                            logger.warning(f"Failed to parse diff for {key}: {e}")
                            continue

                if side_by_side:
                    diffs["mismatched"].append({
                        "object": key,
                        "diffs": side_by_side
                    })

        for key in dst_objs:
            if key not in src_objs:
                diffs["extra_in_dest"].append(key)

        # Message if no diffs found
        if not diffs["missing_in_dest"] and not diffs["extra_in_dest"] and not diffs["mismatched"]:
            diffs["info"] = "No Missing, Extras and Mismatches."
        result[obj_type] = diffs

    logger.info("Metadata comparison completed.")
    return result
