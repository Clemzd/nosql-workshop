var Installation = React.createClass({
    render: function () {
        var Glyphicon = ReactBootstrap.Glyphicon;
        var equipements = this.props.data.equipements.map(function (equipement) {
            var activites = '';
            if (equipement.activites) {
                activites = equipement.activites.join(' ; ');
            }
            return (
                <tr>
                    <td>{equipement.numero}</td>
                    <td>{equipement.nom}</td>
                    <td>{equipement.type}</td>
                    <td>{equipement.famille}</td>
                    <td>{activites}</td>
                </tr>
            );
        });
        return (
            <div className="panel panel-primary">
                <div className="panel-heading">
                    <h3 className="panel-title">{'\u0023 ' + this.props.data._id + ' - ' + this.props.data.nom}</h3>
                </div>
                <div className="panel-body">
                    <p>
                        <Glyphicon glyph="envelope" />
                        {'\u00A0' + this.props.data.adresse.numero + ' ' + this.props.data.adresse.voie + ' ' + this.props.data.adresse.codePostal + ' ' + this.props.data.adresse.commune}
                    </p>
                    <p>
                        <Glyphicon glyph="map-marker" />
                        {'\u00A0 lat : ' + this.props.data.location.coordinates[0] + ' ; lon : ' + this.props.data.location.coordinates[1]}
                    </p>
                    <p>
                        <Glyphicon glyph="road" />
                        {'\u00A0' + this.props.data.nbPlacesParking + ' place(s) de parking'}
                    </p>
                    <hr/>
                    <p>
                        <b>Equipements</b>
                    </p>
                    <table className="table table-condensed">
                        <thead>
                            <tr>
                                <th>{'\u0023'}</th>
                                <th>Nom</th>
                                <th>Type</th>
                                <th>Famille</th>
                                <th>Activités</th>
                            </tr>
                        </thead>
                        <tbody>
                            {equipements}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    }
});
