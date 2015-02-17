var InstallationList = React.createClass({
    render: function () {
        var installations = this.props.data.map(function (installation) {
            return (
                <Installation data={installation} />
            );
        });
        return (
            <div>
                {installations}
            </div>
        );
    }
});
