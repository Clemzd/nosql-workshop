package nosql.workshop.model.stats;

import nosql.workshop.model.Installation;

import java.util.List;

public class InstallationsStats {

    private long totalCount;
    private List<CountByActivity> countByActivity;
    private Installation installationWithMaxEquipments;
    private double averageEquipmentsPerInstallation;

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public List<CountByActivity> getCountByActivity() {
        return countByActivity;
    }

    public void setCountByActivity(List<CountByActivity> countByActivity) {
        this.countByActivity = countByActivity;
    }

    public Installation getInstallationWithMaxEquipments() {
        return installationWithMaxEquipments;
    }

    public void setInstallationWithMaxEquipments(Installation installationWithMaxEquipments) {
        this.installationWithMaxEquipments = installationWithMaxEquipments;
    }

    public double getAverageEquipmentsPerInstallation() {
        return averageEquipmentsPerInstallation;
    }

    public void setAverageEquipmentsPerInstallation(double averageEquipmentsPerInstallation) {
        this.averageEquipmentsPerInstallation = averageEquipmentsPerInstallation;
    }
}
