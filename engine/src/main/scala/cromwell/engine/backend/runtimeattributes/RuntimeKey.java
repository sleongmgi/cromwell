package cromwell.engine.backend.runtimeattributes;

import cromwell.engine.backend.BackendType;
import static cromwell.engine.backend.BackendType.JES;
import static cromwell.engine.backend.BackendType.LOCAL;
import static cromwell.engine.backend.BackendType.SGE;
import static cromwell.engine.backend.BackendType.LSF;
/**
 * Backend runtime keys and the backends which are known to support them.
 */
public enum RuntimeKey {
    CONTINUE_ON_RETURN_CODE("continueOnReturnCode", LOCAL, SGE, JES, LSF),
    CPU("cpu", JES),
    DISKS("disks", JES),
    ZONES("zones", JES),
    DOCKER("docker", new BackendType[]{JES}, LOCAL), // Alternate constructor due to both optional and mandatory backends
    FAIL_ON_STDERR("failOnStderr", JES, LOCAL, SGE, LSF),
    MEMORY("memory", JES),
    PREEMPTIBLE("preemptible", JES),
    BOOT_DISK("bootDiskSizeGb", JES);

    public final String key;

    /**
     * BackendTypes on which this key is mandatory.
     */
    public final BackendType[] mandatory;

    /**
     * BackendTypes optionally supporting this key.  A BackendType need only appear in one
     * of the mandatory or optional arrays.
     */
    public final BackendType[] optional;

    RuntimeKey(String key, BackendType... optional) {
        this(key, new BackendType[0], optional);
    }

    RuntimeKey(String key, BackendType [] mandatory, BackendType... optional) {
        this.key = key;
        this.mandatory = mandatory;
        this.optional = optional;
    }

    public boolean isMandatory(BackendType backendType) {
        for (BackendType type : mandatory) {
            if (type == backendType) {
                return true;
            }
        }
        return false;
    }

    public boolean isOptional(BackendType backendType) {
        for (BackendType type : optional) {
            if (type == backendType) {
                return true;
            }
        }
        return false;
    }

    public static RuntimeKey from(String str) {
        for (RuntimeKey k : RuntimeKey.values()) {
            if (str.equalsIgnoreCase(k.key)) return k;
        }
        throw new UnsupportedOperationException("Runtime key " + str + " is not valid");
    }
    /**
     * Returns true if the specified key is mandatory or optional on this backend.
     */
    public boolean supports(BackendType backendType) {
        return isOptional(backendType) || isMandatory(backendType);
    }
}
